"""Kafka consumer of the Temperature Observability service.

Several of these can run in the same consumer group. All events of an
experiment are in the same partition (the producer keys them by experiment id),
so whichever consumer owns the partition owns the experiment. To make handing a
partition to another consumer safe (scaling, crashes):

- The state of every experiment (config, phase, out-of-range flag and the offset
  of the last event that changed it) is in the `experiments` table. A consumer
  that gets a partition loads its experiments from there.
- Readings of a measurement that is not complete yet only live in memory, so
  offsets are committed only up to the first reading of the oldest incomplete
  measurement. The next owner reads those readings again.
- Events up to the last change (offset <= the experiment's last offset) are
  skipped. Measurements after it are processed again, but none of them changes
  the state, measurements are inserted with ON CONFLICT DO NOTHING and the
  notifications service ignores duplicate notifications. Reading an event twice
  therefore has no effect.
"""
from collections import defaultdict
import sys
import io
import logging
import json
import time
import click
from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer, KafkaException, TopicPartition
import psycopg2
import signal
import os
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from notifier import default_notifier

running = True

def signal_handler(sig, frame):
    # finish the current message and commit, instead of exiting halfway through it
    global running
    logger.info("Interrupt received, grecafully shutting down.")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

fmt = "\033[36m%(asctime)s\033[0m [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=fmt, datefmt="%H:%M:%S")

logging.addLevelName(logging.INFO, "\033[32mINFO\033[0m")       # Green
logging.addLevelName(logging.WARNING, "\033[33mWARN\033[0m")    # Yellow
logging.addLevelName(logging.ERROR, "\033[31mERROR\033[0m")     # Red
logger = logging.getLogger(__name__)

DB_CONFIG = "dbname=temp_db user=postgres password=cec host=mypg"
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
# seconds between offset commits
COMMIT_INTERVAL = 1.0
# a measurement that is still missing readings after this many seconds is dropped,
# e.g. because the consumer started in the middle of it
STALE_MEASUREMENT_SECONDS = 30.0

# Prometheus metrics, scraped from :METRICS_PORT/metrics
MESSAGES = Counter("consumer_messages_total", "Kafka messages consumed", ["event_type"])
SKIPPED = Counter("consumer_skipped_messages_total", "Messages that had no effect", ["reason"])
PROCESSING_TIME = Histogram(
    "consumer_message_processing_seconds",
    "Time to decode and process one Kafka message, including the DB write",
    buckets=(0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
)
DB_WRITE_TIME = Histogram(
    "consumer_db_write_seconds",
    "Time of one round trip to Postgres",
    buckets=(0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25),
)
CONSUMER_LAG = Gauge(
    "consumer_lag_messages",
    "Messages in the partition that this consumer has not processed yet",
    ["topic", "partition"],
)
ACTIVE_EXPERIMENTS = Gauge("consumer_active_experiments", "Experiments this consumer is tracking")
ASSIGNED_PARTITIONS = Gauge("consumer_assigned_partitions", "Partitions assigned to this consumer")
TOPIC_PARTITIONS = Gauge("consumer_topic_partitions", "Partitions of the topic, the most consumers that can be busy")
REBALANCES = Counter("consumer_rebalances_total", "Partition assignment changes", ["change"])

# Kept in sync with db_queries.sql, created here as well because the database
# volume of an existing deployment does not run the init script again
EXPERIMENTS_DDL = """
CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,
    researcher TEXT,
    lower_threshold DOUBLE PRECISION NOT NULL,
    upper_threshold DOUBLE PRECISION NOT NULL,
    num_sensors INTEGER NOT NULL,
    phase TEXT NOT NULL,
    out_of_range BOOLEAN NOT NULL,
    last_offset BIGINT NOT NULL
)
"""

def report_lag(stats_json):
    """librdkafka statistics callback, called from poll() every statistics.interval.ms."""
    stats = json.loads(stats_json)
    # partitions that were revoked in a rebalance must not keep their last value
    CONSUMER_LAG.clear()
    for topic, topic_stats in stats.get("topics", {}).items():
        for partition, partition_stats in topic_stats.get("partitions", {}).items():
            # librdkafka's consumer_lag counts from the committed offset, which trails
            # by the commit interval and incomplete measurements; count from what we
            # have read instead, once we have read anything from the partition
            # partitions handed to another consumer stay in the stats with their last position
            if partition == "-1" or partition_stats.get("fetch_state") != "active":
                continue
            hi_offset = partition_stats.get("hi_offset", -1)
            app_offset = partition_stats.get("app_offset", -1)
            lag = hi_offset - app_offset if hi_offset >= 0 and app_offset >= 0 else partition_stats.get("consumer_lag", -1)
            if lag >= 0:
                CONSUMER_LAG.labels(topic, partition).set(lag)

def report_commit(err, partitions):
    if err is not None:
        logger.warning("Offset commit failed: %s", err)

class Experiment():
    def __init__(self, experiment_id, researcher, lower_threshold, upper_threshold, num_sensors,
                 phase, out_of_range, last_offset, partition):
        self.experiment_id = experiment_id
        self.researcher = researcher
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.num_sensors = num_sensors
        self.phase = phase
        # whether the last measurement was out of range, used to notify only on a change
        self.out_of_range = out_of_range
        # offset of the last event that changed this experiment
        self.last_offset = last_offset
        self.partition = partition
        # measurement_id -> {sensor: temperature} of measurements that are missing readings
        self.partial = {}

    def in_range(self, temperature):
        return self.lower_threshold <= temperature <= self.upper_threshold

class Database():
    """One Postgres connection that reconnects once when it was lost."""
    def __init__(self):
        self.conn = None
        self.cur = None
        self.connect()
        self.execute("SELECT pg_advisory_lock(4711)")
        self.execute(EXPERIMENTS_DDL)
        self.execute("SELECT pg_advisory_unlock(4711)")

    def connect(self):
        self.conn = psycopg2.connect(DB_CONFIG)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def execute(self, sql, params=None):
        for attempt in (1, 2):
            try:
                with DB_WRITE_TIME.time():
                    self.cur.execute(sql, params)
                return self.cur
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt == 2:
                    raise
                logger.warning("Lost the database connection (%s), reconnecting", e)
                self.connect()

    def close(self):
        self.cur.close()
        self.conn.close()

class State():
    """The experiments of the assigned partitions and which offsets are safe to commit."""
    def __init__(self, db, topic):
        self.db = db
        self.topic = topic
        self.experiments = {}
        # experiments the database doesn't know, so their events are skipped without a query
        self.unknown = set()
        # partition -> {(experiment_id, measurement_id): (offset of its first reading, when it was read)}
        # in offset order, so the first entry is the oldest incomplete measurement
        self.open = defaultdict(dict)
        # partition -> offset after the last processed message
        self.next_offset = {}
        # partition -> last committed offset
        self.committed = {}
        # set when processing a message failed halfway, e.g. a DB write: nothing may be
        # committed any more, because the message's effects are only partly stored
        self.failed = False

    # --- experiments ---------------------------------------------------------

    def load(self, experiment_id, partition):
        row = self.db.execute(
            """
            SELECT researcher, lower_threshold, upper_threshold, num_sensors, phase, out_of_range, last_offset
            FROM experiments WHERE experiment_id = %s
            """,
            (experiment_id,),
        ).fetchone()
        if row is None or row[4] == "experiment_terminated":
            if row is None:
                logger.warning("Skipping events of unknown experiment %s", experiment_id)
            self.unknown.add(experiment_id)
            return None
        experiment = Experiment(experiment_id, *row, partition=partition)
        self.experiments[experiment_id] = experiment
        logger.info("Loaded experiment %s (%s) from the database", experiment_id, experiment.phase)
        return experiment

    def get(self, experiment_id, partition):
        experiment = self.experiments.get(experiment_id)
        if experiment is None and experiment_id not in self.unknown:
            experiment = self.load(experiment_id, partition)
        return experiment

    # --- events --------------------------------------------------------------

    def process(self, event_type, record, partition, offset):
        experiment_id = record["experiment"]
        if event_type == "experiment_configured":
            self.configure(record, partition, offset)
        elif (experiment := self.get(experiment_id, partition)) is None:
            SKIPPED.labels("unknown_experiment").inc()
        elif offset <= experiment.last_offset:
            SKIPPED.labels("already_processed").inc()
        elif event_type == "stabilization_started":
            # the first measurement in range is a Stabilized notification
            self.set_phase(experiment, event_type, True, offset)
        elif event_type == "experiment_started":
            # the first measurement out of range is an OutOfRange notification
            self.set_phase(experiment, event_type, False, offset)
        elif event_type == "sensor_temperature_measured":
            self.reading(experiment, record, partition, offset)
        elif event_type == "experiment_terminated":
            self.terminate(experiment, record, partition, offset)
        # only now, a message that was interrupted is read again by the next owner
        self.next_offset[partition] = offset + 1

    def configure(self, record, partition, offset):
        experiment_id = record["experiment"]
        if experiment_id in self.experiments:
            SKIPPED.labels("already_processed").inc()
            return
        temp_range = record["temperature_range"]
        researcher = record.get("researcher")
        num_sensors = len(record["sensors"])
        inserted = self.db.execute(
            """
            INSERT INTO experiments
            (experiment_id, researcher, lower_threshold, upper_threshold, num_sensors, phase, out_of_range, last_offset)
            VALUES (%s, %s, %s, %s, %s, 'experiment_configured', FALSE, %s)
            ON CONFLICT (experiment_id) DO NOTHING
            RETURNING experiment_id
            """,
            (experiment_id, researcher, temp_range["lower_threshold"], temp_range["upper_threshold"], num_sensors, offset),
        ).fetchone()
        self.unknown.discard(experiment_id)
        if inserted is None:
            # configured before, by us or a previous owner of the partition
            SKIPPED.labels("already_processed").inc()
            self.load(experiment_id, partition)
            return
        self.experiments[experiment_id] = Experiment(
            experiment_id, researcher, temp_range["lower_threshold"], temp_range["upper_threshold"],
            num_sensors, "experiment_configured", False, offset, partition,
        )
        logger.info("Configured experiment: %s (researcher: %s)", experiment_id, researcher)

    def set_phase(self, experiment, phase, out_of_range, offset):
        experiment.phase = phase
        experiment.out_of_range = out_of_range
        experiment.last_offset = offset
        self.db.execute(
            "UPDATE experiments SET phase = %s, out_of_range = %s, last_offset = %s WHERE experiment_id = %s AND last_offset < %s",
            (phase, out_of_range, offset, experiment.experiment_id, offset),
        )
        logger.info("Experiment %s transitioned to %s", experiment.experiment_id, phase)

    def reading(self, experiment, record, partition, offset):
        measurement_id = record["measurement_id"]
        readings = experiment.partial.get(measurement_id)
        if readings is None:
            readings = experiment.partial[measurement_id] = {}
            self.open[partition][(experiment.experiment_id, measurement_id)] = (offset, time.monotonic())
        # per sensor, so a reading that is read twice is not averaged twice
        readings[record["sensor"]] = record["temperature"]
        if len(readings) < experiment.num_sensors:
            return

        del experiment.partial[measurement_id]
        del self.open[partition][(experiment.experiment_id, measurement_id)]
        avg_temp = sum(readings.values()) / experiment.num_sensors
        out_of_range = not experiment.in_range(avg_temp)
        logger.debug("Avg. temperature for experiment %s (measurement %s): %s", experiment.experiment_id, measurement_id, avg_temp)

        # Same rules as the producer uses to decide which measurements are notified:
        # a change into the range while stabilizing, out of it while running
        notify = None
        if experiment.phase == "stabilization_started" and experiment.out_of_range and not out_of_range:
            notify = default_notifier.notify_stabilized
            logger.info("Stabilization reached for experiment %s", experiment.experiment_id)
        elif experiment.phase == "experiment_started" and out_of_range and not experiment.out_of_range:
            notify = default_notifier.notify_out_of_range
            logger.warning("OUT OF RANGE for %s: %f", experiment.experiment_id, avg_temp)
        # The experiment row is only updated when the flag changes. Replaying the
        # measurements after the last change gives the same result: none of them
        # changes the flag, and their rows are ignored as duplicates.
        changed = out_of_range != experiment.out_of_range
        experiment.out_of_range = out_of_range
        experiment.last_offset = offset

        # before the DB write: if we crash in between, the next owner sends it again,
        # and the notifications service ignores the duplicate
        if notify is not None:
            notify(
                researcher=experiment.researcher,
                experiment_id=experiment.experiment_id,
                measurement_id=measurement_id,
                cipher_data=record["measurement_hash"],
            )

        # a failed write raises and stops the consumer without committing (see
        # State.failed), so the measurement is read again after the restart
        if experiment.phase == "experiment_started" and changed:
            # stored for the REST API, in one round trip and one transaction with the new state
            self.db.execute(
                """
                WITH inserted AS (
                    INSERT INTO measurements
                    (measurement_id, experiment_id, timestamp, temperature, measurement_hash, out_of_range)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (measurement_id) DO NOTHING
                )
                UPDATE experiments SET out_of_range = %s, last_offset = %s WHERE experiment_id = %s AND last_offset < %s
                """,
                (
                    measurement_id, experiment.experiment_id, record["timestamp"], avg_temp,
                    record["measurement_hash"], out_of_range,
                    out_of_range, offset, experiment.experiment_id, offset,
                ),
            )
        elif experiment.phase == "experiment_started":
            self.db.execute(
                """
                INSERT INTO measurements
                (measurement_id, experiment_id, timestamp, temperature, measurement_hash, out_of_range)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (measurement_id) DO NOTHING
                """,
                (measurement_id, experiment.experiment_id, record["timestamp"], avg_temp,
                 record["measurement_hash"], out_of_range),
            )
        elif changed:
            # stabilization measurements are not stored
            self.db.execute(
                "UPDATE experiments SET out_of_range = %s, last_offset = %s WHERE experiment_id = %s AND last_offset < %s",
                (out_of_range, offset, experiment.experiment_id, offset),
            )

    def terminate(self, experiment, record, partition, offset):
        logger.info("Experiment %s terminated.", experiment.experiment_id)
        self.db.execute(
            """
            WITH terminated AS (
                INSERT INTO experiments_terminated (experiment_id, terminated_at)
                VALUES (%s, %s)
                ON CONFLICT (experiment_id) DO NOTHING
            )
            UPDATE experiments SET phase = 'experiment_terminated', last_offset = %s
            WHERE experiment_id = %s AND last_offset < %s
            """,
            (experiment.experiment_id, record["timestamp"], offset, experiment.experiment_id, offset),
        )
        for measurement_id in experiment.partial:
            self.open[partition].pop((experiment.experiment_id, measurement_id), None)
        del self.experiments[experiment.experiment_id]

    # --- offsets and rebalancing ---------------------------------------------

    def safe_offset(self, partition):
        """Everything before this offset is applied, readings of incomplete measurements are not."""
        open_measurements = self.open.get(partition)
        if open_measurements:
            return next(iter(open_measurements.values()))[0]
        return self.next_offset[partition]

    def commit(self, consumer, partitions=None, asynchronous=True):
        if self.failed:
            return
        partitions = self.next_offset.keys() if partitions is None else partitions
        offsets = []
        for partition in partitions:
            if partition not in self.next_offset:
                continue
            offset = self.safe_offset(partition)
            if self.committed.get(partition) != offset:
                offsets.append(TopicPartition(self.topic, partition, offset))
                self.committed[partition] = offset
        if not offsets:
            return
        try:
            consumer.commit(offsets=offsets, asynchronous=asynchronous)
        except KafkaException as e:
            logger.warning("Offset commit failed: %s", e)

    def drop_stale(self):
        """Drops measurements that will never be complete, so they don't hold back the commits."""
        cutoff = time.monotonic() - STALE_MEASUREMENT_SECONDS
        for open_measurements in self.open.values():
            stale = [key for key, (_, seen) in open_measurements.items() if seen < cutoff]
            for experiment_id, measurement_id in stale:
                del open_measurements[(experiment_id, measurement_id)]
                if experiment := self.experiments.get(experiment_id):
                    experiment.partial.pop(measurement_id, None)
                SKIPPED.labels("incomplete_measurement").inc()
                logger.warning("Dropped incomplete measurement %s of experiment %s", measurement_id, experiment_id)

    def forget(self, partitions):
        """Drops everything about partitions that another consumer owns from now on."""
        for partition in partitions:
            self.open.pop(partition, None)
            self.next_offset.pop(partition, None)
            self.committed.pop(partition, None)
        self.experiments = {
            experiment_id: experiment
            for experiment_id, experiment in self.experiments.items()
            if experiment.partition not in partitions
        }
        # the new owners may configure them
        self.unknown.clear()

@click.command()
@click.argument('topic')
def consume(topic: str):
    consumer = Consumer({
        'bootstrap.servers': 'kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092',
        'group.id': "my_group_1",   # Keep the Group ID constant but change it for each iteration
        'auto.offset.reset': 'latest',
        # committed by State.commit, only up to what is safe to skip
        'enable.auto.commit': 'false',
        # a rebalance only moves the partitions it has to
        'partition.assignment.strategy': 'cooperative-sticky',
        # partitions of a crashed consumer move after 6 s (the broker's minimum) instead of 45 s; heartbeats
        # come from a librdkafka thread, so slow processing doesn't trigger this. Members notice
        # a rebalance at their next heartbeat, and a cooperative one takes two rounds, so
        # frequent heartbeats make scaling pause the moved partitions for less time.
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 1000,
        'security.protocol': 'SSL',
        'ssl.ca.location': './auth/ca.crt',
        'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
        'ssl.keystore.password': 'cc2023',
        'ssl.endpoint.identification.algorithm': 'none',
        # lag every 2 s, the autoscaler reacts to it
        'statistics.interval.ms': 2000,
        'stats_cb': report_lag,
        'on_commit': report_commit,
    })

    db = Database()
    state = State(db, topic)
    assigned = set()

    def on_assign(_, partitions):
        assigned.update(p.partition for p in partitions)
        ASSIGNED_PARTITIONS.set(len(assigned))
        REBALANCES.labels("assigned").inc(len(partitions))
        logger.info("Assigned partitions %s, now %s", [p.partition for p in partitions], sorted(assigned))

    def on_revoke(_, partitions):
        revoked = {p.partition for p in partitions}
        # last chance to commit before the next owner starts reading
        state.commit(consumer, revoked, asynchronous=False)
        state.forget(revoked)
        assigned.difference_update(revoked)
        ASSIGNED_PARTITIONS.set(len(assigned))
        REBALANCES.labels("revoked").inc(len(partitions))
        logger.info("Revoked partitions %s, now %s", sorted(revoked), sorted(assigned))

    def on_lost(_, partitions):
        # already owned by someone else, so committing would fail
        lost = {p.partition for p in partitions}
        state.forget(lost)
        assigned.difference_update(lost)
        ASSIGNED_PARTITIONS.set(len(assigned))
        REBALANCES.labels("lost").inc(len(partitions))
        logger.warning("Lost partitions %s", sorted(lost))

    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    TOPIC_PARTITIONS.set(len(consumer.list_topics(topic, timeout=10).topics[topic].partitions))

    ACTIVE_EXPERIMENTS.set_function(lambda: len(state.experiments))
    start_http_server(METRICS_PORT)

    last_commit = time.monotonic()
    try:
        while running:
            msg = consumer.poll(0.5)
            if msg is not None and msg.error():
                logger.error(f"Consumer error: {msg.error()}")
            elif msg is not None:
                kafka_headers = dict(msg.headers() or [])
                event_type = kafka_headers.get("record_name", b"").decode("utf-8")

                MESSAGES.labels(event_type).inc()
                with PROCESSING_TIME.time():
                    with DataFileReader(io.BytesIO(msg.value()), DatumReader()) as reader:
                        record = next(reader)
                    state.process(event_type, record, msg.partition(), msg.offset())

            if time.monotonic() - last_commit >= COMMIT_INTERVAL:
                state.drop_stale()
                state.commit(consumer)
                last_commit = time.monotonic()

    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
    except Exception:
        # exit without committing; the container restarts and the next owner of the
        # partitions reads the message again
        state.failed = True
        logger.exception("Stopping after an error, without committing")
        raise
    finally:
        print("Closing consumer and database connection...")
        state.commit(consumer, asynchronous=False)
        consumer.close()
        db.close()
        default_notifier.shutdown(wait=True)
        default_notifier.log_stats_summary()

if __name__ == '__main__':
    consume()

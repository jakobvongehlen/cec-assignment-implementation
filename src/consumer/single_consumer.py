from collections import defaultdict
import io
import logging
import json
import random
import click
from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer
import psycopg2

from notifier import default_notifier

fmt = "\033[36m%(asctime)s\033[0m [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=fmt, datefmt="%H:%M:%S")

logging.addLevelName(logging.INFO, "\033[32mINFO\033[0m")       # Green
logging.addLevelName(logging.WARNING, "\033[33mWARN\033[0m")    # Yellow
logging.addLevelName(logging.ERROR, "\033[31mERROR\033[0m")     # Red
logger = logging.getLogger(__name__)

DB_CONFIG = "dbname=temp_db user=postgres password=cec host=mypg"

class Experiment():
    def __init__(self, experiment_id, tmp_upper_threshold, tmp_lower_threshold, num_sensors, researcher=None):
        self.experiment_id = experiment_id
        self.researcher = researcher
        self.num_sensors = num_sensors
        self.tmp_upper_threshold = tmp_upper_threshold
        self.tmp_lower_threshold = tmp_lower_threshold

        self.out_of_range = True
        self.phase = "experiment_configured"

        self.stabilized = False
        self.measurements = defaultdict(list)

def persist_measurement(cur, record, avg_temp, m_hash, out_of_range):
    """Persists the average temperature measurement to the database."""
    m_hash = record["measurement_hash"]
    cur.execute(
        """
        INSERT INTO measurements 
        (measurement_id, experiment_id, timestamp, temperature, measurement_hash, out_of_range) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            record["measurement_id"],
            record["experiment"],
            record["timestamp"],
            avg_temp,
            m_hash,
            out_of_range,
        ),
    )

def persist_experiment_termination(cur, experiment_id, terminated_at):
    """Persists the experiment termination event to the database."""
    cur.execute(
        """
        INSERT INTO experiments_terminated 
        (experiment_id, terminated_at) 
        VALUES (%s, %s)
        """,
        (experiment_id, terminated_at),
    )

def process_measurement(record, event_type, experiment, cur):
    """Processes temperature readings, checks experiment stabilization, and aggregates sensors."""
    temp = record.get("temperature")

    # Handle sensor aggregation
    m_hash = record["measurement_hash"]
    measurement_id = record["measurement_id"]
    experiment.measurements[measurement_id].append(temp)

    if len(experiment.measurements[measurement_id]) == experiment.num_sensors:
        temps = experiment.measurements.pop(measurement_id)
        avg_temp = sum(temps) / experiment.num_sensors

        # Check for stabilization
        if (
            experiment.phase == "stabilization_started"
            and avg_temp is not None
            and experiment.stabilized is False
            and experiment.tmp_lower_threshold <= avg_temp <= experiment.tmp_upper_threshold
        ):
            experiment.stabilized = True
            experiment.out_of_range = False
            logger.info("Stabilization reached for experiment %s", experiment.experiment_id)
            
            default_notifier.notify_stabilized(
                researcher=experiment.researcher,
                experiment_id=experiment.experiment_id,
                measurement_id=measurement_id,
                cipher_data=m_hash,
            )
            return experiment

        logger.info(f"Avg. temperature for experiment {experiment.experiment_id} (measurement {measurement_id}): {avg_temp}")

        if experiment.phase == "experiment_started":
            out_of_range = not (experiment.tmp_lower_threshold <= avg_temp <= experiment.tmp_upper_threshold)

            # Handle out-of-range state changes
            if out_of_range and experiment.out_of_range is False:
                logger.warning("OUT OF RANGE for %s: %f", experiment.experiment_id, avg_temp)
                # NOTIFY THE NOTIFICATION SERVICE
                default_notifier.notify_out_of_range(
                    researcher=experiment.researcher,
                    experiment_id=experiment.experiment_id,
                    measurement_id=measurement_id,
                    cipher_data=m_hash,
                )
                experiment.out_of_range = True

            # Handle transition back to in-range
            elif not out_of_range and experiment.out_of_range is True:
                experiment.out_of_range = False

            # Store the measurement in the database enabling the REST API to query it later
            try:
                persist_measurement(cur, record, avg_temp, m_hash, out_of_range)
            except Exception as e:
                logger.error("DB insert failed for experiment %s: %s", experiment.experiment_id, e)

    return experiment

def process_event(event_type: str, record, experiments: dict, cur):
    """Routes events based on their type."""
    experiment_id = record.get("experiment")

    match event_type:
        case "experiment_configured":
            temp_range = record.get("temperature_range") or {}
            sensors = record.get("sensors") or []
            researcher = record.get("researcher")
            experiments[experiment_id] = Experiment(
                experiment_id=experiment_id,
                tmp_upper_threshold=temp_range.get("upper_threshold"),
                tmp_lower_threshold=temp_range.get("lower_threshold"),
                num_sensors=len(sensors),
                researcher=researcher,
            )
            logger.info("Configured experiment: %s (researcher: %s)", experiment_id, researcher)

        case "stabilization_started" | "experiment_started":
            if exp := experiments.get(experiment_id):
                exp.phase = event_type
                logger.info(f"Experiment {experiment_id} transitioned to {event_type}")
            else:
                logger.warning(f"Received {event_type} for unknown experiment {experiment_id}")

        case "sensor_temperature_measured":
            if exp := experiments.get(experiment_id):
                process_measurement(record, event_type, exp, cur)
            else:
                logger.warning(f"Received measurement for unknown experiment {experiment_id}")

        case "experiment_terminated":
            logger.info(f"Experiment {experiment_id} terminated.")
            try:
                persist_experiment_termination(cur, experiment_id, record.get("timestamp"))
            except Exception as e:
                logger.error("DB insert failed for experiment termination %s: %s", experiment_id, e)
            experiments.pop(experiment_id, None)

@click.command()
@click.argument('topic')
def consume(topic: str):
    consumer = Consumer({
        'bootstrap.servers': 'kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092',
        'group.id': f"{random.random()}",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'true',
        'security.protocol': 'SSL',
        'ssl.ca.location': './auth/ca.crt',
        'ssl.keystore.location': './auth/kafka.keystore.pkcs12',
        'ssl.keystore.password': 'cc2023',
        'ssl.endpoint.identification.algorithm': 'none',
    })
    consumer.subscribe([topic], on_assign=lambda _, p_list: logger.info(p_list))

    conn = psycopg2.connect(DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    experiments = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            kafka_headers = dict(msg.headers() or [])
            event_type = kafka_headers.get("record_name", b"").decode("utf-8")

            with DataFileReader(io.BytesIO(msg.value()), DatumReader()) as reader:
                record = next(reader)
            process_event(event_type, record, experiments, cur)

    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
    finally:
        consumer.close()
        cur.close()
        conn.close()
        default_notifier.log_stats_summary()
        default_notifier.shutdown(wait=False)

if __name__ == '__main__':
    consume()
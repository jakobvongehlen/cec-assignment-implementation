from collections import defaultdict
import io
import json
import random
import click
from avro.datafile import DataFileReader
from avro.io import DatumReader
from confluent_kafka import Consumer
import psycopg2

class Experiment():
    def __init__(self, experiment_id, upper_threshold, lower_threshold, num_sensors):
        self.experiment_id = experiment_id
        self.researcher = None
        self.num_sensors = num_sensors
        self.tmp_upper_threshold = upper_threshold
        self.tmp_lower_threshold = lower_threshold

        self.phase = "experiment_configured"

        self.stabilized = False
        self.measurements = defaultdict(list)

def process_measurement(record, event_type, experiment, cur):
    """Processes an individual event."""
    temp = record.get("temperature")

    # Check for stabilization
    if (
        experiment.phase == "stabilization_started"
        and temp is not None
        and experiment.stabilized is False
        and experiment.tmp_lower_threshold <= temp <= experiment.tmp_upper_threshold
    ):
        experiment.stabilized = True
        print("Stabilization reached")
        # TODO: NOTIFY THE NOTIFICATION SERVICE

    # Handle sensor aggregation
    m_hash = record["measurement_hash"]
    experiment.measurements[m_hash].append(temp)

    if len(experiment.measurements[m_hash]) == experiment.num_sensors:
        temps = experiment.measurements.pop(m_hash)
        avg_temp = sum(temps) / experiment.num_sensors
        print(f"Average temperature for measurement {m_hash}: {avg_temp}\n\n")

        if experiment.phase == "experiment_started":
            out_of_range = not (experiment.tmp_lower_threshold <= avg_temp <= experiment.tmp_upper_threshold)
            if out_of_range:
                print("OUT OF RANGE")
                # TODO: NOTIFY THE NOTIFICATION SERVICE

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

    return experiment

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
    consumer.subscribe([topic], on_assign=lambda _, p_list: print(p_list))

    conn = psycopg2.connect("dbname=temp_db user=postgres password=cec host=mypg")
    conn.autocommit = True
    cur = conn.cursor()

    experiments = {}

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            kafka_headers = dict(msg.headers() or [])
            event_type = kafka_headers.get("record_name", b"").decode("utf-8")

            with DataFileReader(io.BytesIO(msg.value()), DatumReader()) as reader:
                record = next(reader)
                experiment_id = record.get("experiment")

                if (event_type == "experiment_configured"):
                    temp_range = record.get("temperature_range") or {}
                    sensors = record.get("sensors") or []

                    experiments[experiment_id] = Experiment(
                        experiment_id,
                        temp_range.get("upper_threshold", None),
                        temp_range.get("lower_threshold", None),
                        len(sensors)
                    )
                    print(f"Experiment {experiment_id} has configured.")

                elif (event_type == "stabilization_started"):
                    if experiment_id in experiments:
                        experiments[experiment_id].phase = "stabilization_started"
                        print(f"Experiment {experiment_id} has started stabilization.")
                    else:
                        print(f"Received stabilization_started for unknown experiment {experiment_id}.")

                elif (event_type == "experiment_started"):
                    if experiment_id in experiments:
                        experiments[experiment_id].phase = "experiment_started"
                        print(f"Experiment {experiment_id} has started.")
                    else:
                        print(f"Received experiment_started for unknown experiment {experiment_id}.")

                elif (event_type == "sensor_temperature_measured"):
                    experiments[experiment_id] = process_measurement(record, event_type, experiments[experiment_id], cur)
                    
                else:
                    print("Experiment terminated.\n\n")
                    # TODO: NOTIFY THE NOTIFICATION SERVICE

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        cur.close()
        conn.close()

if __name__ == '__main__':
    consume()
import signal
import click
import random
from avro.datafile import DataFileReader
from avro.io import DatumReader
import io
import json
from confluent_kafka import Consumer
import psycopg2

NUM_SENSORS = 2
LOWER_THRESHOLD = 100
UPPER_THRESHOLD = 150

c = Consumer({
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

conn = psycopg2.connect("dbname=temp_db user=postgres password=cec host=mypg")
conn.autocommit = True
cur = conn.cursor()


@click.command()
@click.argument('topic')
def consume(topic: str):
    c.subscribe(
        [topic],
        on_assign=lambda _, p_list: print(p_list)
    )

    intermediate_measurements = {}
    phase = None
    stabilization = False

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        headers = dict(msg.headers())
        event_type = headers.get("record_name", b"").decode("utf-8")

        if event_type == "experiment_configured":
            phase = "configured"
            print("Experiment configured")

        phase = event_type if event_type != "sensor_temperature_measured" else phase

        reader = DataFileReader(io.BytesIO(msg.value()), DatumReader())
        print(event_type)

        for record in reader:
            event = json.dumps(record, indent=4)
            print(event)

            if (phase == "stabilization_started" and
                    event_type == "sensor_temperature_measured" and
                    stabilization == False and
                    record["temperature"] >= LOWER_THRESHOLD and
                    record["temperature"] <= UPPER_THRESHOLD):
                stabilization = True
                # TODO: NOTIFY THE NOTIFICATION SERVICE
                print("Stabilization reached")

            if event_type == "sensor_temperature_measured":
                if record["measurement_hash"] not in intermediate_measurements:
                    intermediate_measurements[record["measurement_hash"]] = []

                intermediate_measurements[record["measurement_hash"]].append(
                    (record["sensor"], record["timestamp"], record["temperature"])
                )

                if len(intermediate_measurements[record["measurement_hash"]]) == NUM_SENSORS:
                    avg_temp = sum(
                        temp[2] for temp in intermediate_measurements[record["measurement_hash"]]
                    ) / NUM_SENSORS
                    print(f"Average temperature for measurement {record['measurement_hash']}: {avg_temp}")

                    if phase == "experiment_started":
                        out_of_range = avg_temp < LOWER_THRESHOLD or avg_temp > UPPER_THRESHOLD
                        if out_of_range:
                            print("OUT OF RANGE")
                            # TODO: NOTIFY THE NOTIFICATION SERVICE

                        cur.execute(
                            "INSERT INTO measurements "
                            "(measurement_id, experiment_id, timestamp, temperature, measurement_hash, out_of_range) "
                            "VALUES (%s, %s, %s, %s, %s, %s)",
                            (
                                record["measurement_id"],
                                record["experiment"],
                                record["timestamp"],
                                avg_temp,
                                record["measurement_hash"],
                                out_of_range,
                            ),
                        )

                    del intermediate_measurements[record["measurement_hash"]]

        reader.close()

consume()
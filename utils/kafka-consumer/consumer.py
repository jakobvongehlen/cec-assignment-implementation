#!/usr/bin/env python3
import io
import os
import random
import sys

from confluent_kafka import Consumer
from fastavro import reader

BROKERS = os.environ.get(
    "BROKERS",
    "kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
)
TOPIC = os.environ.get("TOPIC", "client18")
AUTH_DIR = os.environ.get("AUTH_DIR", "./auth")
GROUP_ID = os.environ.get("GROUP_ID", "lab2-{}".format(random.random()))
OFFSET_RESET = os.environ.get("OFFSET_RESET", "latest")

consumer = Consumer(
    {
        "bootstrap.servers": BROKERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": True,
        "security.protocol": "SSL",
        "ssl.ca.location": os.path.join(AUTH_DIR, "ca.crt"),
        "ssl.keystore.location": os.path.join(AUTH_DIR, "kafka.keystore.pkcs12"),
        "ssl.keystore.password": os.environ.get("KEYSTORE_PASSWORD", "cc2023"),
        "ssl.endpoint.identification.algorithm": "none",
    }
)


def header_value(headers, name):
    for key, value in headers or []:
        if key == name:
            if isinstance(value, (bytes, bytearray)):
                return value.decode("utf-8")
            return str(value)
    return None


def main():
    consumer.subscribe(
        [TOPIC],
        on_assign=lambda _, partitions: print("assigned: {}".format(partitions), flush=True),
    )
    print(
        "consuming topic '{}' (group '{}', offset reset '{}')".format(
            TOPIC, GROUP_ID, OFFSET_RESET
        ),
        flush=True,
    )

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("consumer error: {}".format(msg.error()), file=sys.stderr, flush=True)
            continue

        record_name = header_value(msg.headers(), "record_name")
        try:
            records = list(reader(io.BytesIO(msg.value())))
        except Exception as exc:
            print(
                "could not deserialize record_name={}: {}".format(record_name, exc),
                file=sys.stderr,
                flush=True,
            )
            continue

        for record in records:
            print(record_name)
            print(record, flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

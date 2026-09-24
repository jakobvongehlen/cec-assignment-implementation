#!/bin/sh
# Start the Kafka consumer and the experiment-producers for Lab Assignment II.

set -eu

cd "$(dirname "$0")"

AUTH="${AUTH:-$HOME/cec-tutorials/kafka/auth}"
BROKERS="${BROKERS:-kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092}"
TOPIC="${TOPIC:-client18}"
SECRET_KEY="${SECRET_KEY:-QJUHsPhnA0eiqHuJqsPgzhDozYO4f1zh}"
CONSUMER_IMAGE="${CONSUMER_IMAGE:-cec-kafka-consumer}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-dclandau/cec-experiment-producer}"
PRODUCERS="${PRODUCERS:-3}"
DURATION="${DURATION:-0}"

[ -d "$AUTH" ] || { echo "auth dir not found: $AUTH" >&2; exit 1; }

docker build -t "$CONSUMER_IMAGE" .

docker rm -f lab2-consumer >/dev/null 2>&1 || true
docker run -d --name lab2-consumer --rm \
  -e BROKERS="$BROKERS" -e TOPIC="$TOPIC" \
  -v "$AUTH:/app/auth:ro" "$CONSUMER_IMAGE"

sleep 5

i=1
while [ "$i" -le "$PRODUCERS" ]; do
  docker rm -f "lab2-producer-$i" >/dev/null 2>&1 || true
  docker run -d --name "lab2-producer-$i" --rm \
    -v "$AUTH:/experiment-producer/auth:ro" \
    "$PRODUCER_IMAGE" --brokers "$BROKERS" --topic "$TOPIC" --secret-key "$SECRET_KEY"
  i=$((i + 1))
done

if [ "$DURATION" -gt 0 ]; then
  timeout "$DURATION" docker logs -f lab2-consumer || true
  docker rm -f lab2-consumer >/dev/null 2>&1 || true
  i=1
  while [ "$i" -le "$PRODUCERS" ]; do
    docker rm -f "lab2-producer-$i" >/dev/null 2>&1 || true
    i=$((i + 1))
  done
else
  docker logs -f lab2-consumer
fi

#!/bin/sh
# Start the notifications-service and run the short-lived test container RUNS times.

set -eu

cd "$(dirname "$0")"

SECRET_KEY="${SECRET_KEY:-QJUHsPhnA0eiqHuJqsPgzhDozYO4f1zh}"
NETWORK="${NETWORK:-cec-lab}"
SERVICE="${SERVICE:-notifications-service}"
SERVICE_IMAGE="${SERVICE_IMAGE:-dclandau/cec-notifications-service}"
TEST_IMAGE="${TEST_IMAGE:-cec-test-container}"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
RUNS="${RUNS:-3}"

docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK"

docker rm -f "$SERVICE" >/dev/null 2>&1 || true
docker run -d --rm --name "$SERVICE" --network "$NETWORK" -p 3000:3000 \
  "$SERVICE_IMAGE" --secret-key "$SECRET_KEY" --external-ip localhost

sleep 3

docker build -t "$TEST_IMAGE" .

mkdir -p "$OUTPUT_DIR"
: > "$OUTPUT_DIR/response.txt"
ABS_OUT="$(cd "$OUTPUT_DIR" && pwd)"

i=1
while [ "$i" -le "$RUNS" ]; do
  docker run --rm --network "$NETWORK" -v "$ABS_OUT:/output" "$TEST_IMAGE"
  i=$((i + 1))
done

cat "$OUTPUT_DIR/response.txt"

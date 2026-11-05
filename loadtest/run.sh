#!/usr/bin/env bash
# Simultaneous load test of the consumers and the HTTP API, see loadtest/README.md.
#
# Usage: loadtest/run.sh [stages]
#   stages: <experiments>:<requests per second>,...  (default below)
# Settings (environment): STAGE_SECONDS, EXPERIMENT_SECONDS, SENSORS, SUT_CPUS,
#   TOOL_CPUS, LOADTEST_TOPIC, LOADTEST_DOCUMENT_WAIT, LOADTEST_MAX_IN_FLIGHT
set -euo pipefail
cd "$(dirname "$0")/.."

STAGES=${1:-${STAGES:-50:250,100:500,200:1000,400:2000,800:4000,1600:8000}}
STAGE_SECONDS=${STAGE_SECONDS:-90}
EXPERIMENT_SECONDS=${EXPERIMENT_SECONDS:-30}
SENSORS=${SENSORS:-2}

# the system under test gets 2 cores like the group VM, the tools the rest
cores=$(nproc)
if (( cores < 4 )); then
  echo "Needs at least 4 cores: 2 for the system under test, the rest for the load tools" >&2
  exit 1
fi
export SUT_CPUS=${SUT_CPUS:-0,1}
export TOOL_CPUS=${TOOL_CPUS:-2-$((cores - 1))}
export LOADTEST_TOPIC=${LOADTEST_TOPIC:-group11}
export LOADTEST_DOCUMENT_WAIT=${LOADTEST_DOCUMENT_WAIT:-10}
export LOADTEST_MAX_IN_FLIGHT=${LOADTEST_MAX_IN_FLIGHT:-200}
export LOADTEST_STAGE_SECONDS=$STAGE_SECONDS

run=$(date +%Y%m%d-%H%M%S)
export LOADTEST_RUN_DIR=./loadtest/out/$run
# a new consumer group per run, which starts at the end of the topic
export LOADTEST_GROUP=loadtest-http-$run
mkdir -p "$LOADTEST_RUN_DIR"

python3 loadtest/plan.py --stages "$STAGES" --stage-seconds "$STAGE_SECONDS" \
  --experiment-seconds "$EXPERIMENT_SECONDS" --sensors "$SENSORS" \
  --document-wait "$LOADTEST_DOCUMENT_WAIT" \
  --producer-config "$LOADTEST_RUN_DIR/producer-config.json" --plan "$LOADTEST_RUN_DIR/plan.json"
LOADTEST_RATES=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["rate_schedule"])' "$LOADTEST_RUN_DIR/plan.json")
duration=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["duration"])' "$LOADTEST_RUN_DIR/plan.json")
export LOADTEST_RATES

compose=(docker compose -f compose.yml -f loadtest/compose.yml)
tools=(experiment-producer http-load-generator)

echo "== Building"
"${compose[@]}" --profile loadtest build "${tools[@]}"

echo "== Starting the system under test on cores $SUT_CPUS (topic $LOADTEST_TOPIC)"
"${compose[@]}" up -d --build

stop_tools() {
  "${compose[@]}" logs --no-color "${tools[@]}" > "$LOADTEST_RUN_DIR/tools.log" 2>&1 || true
  "${compose[@]}" stop "${tools[@]}" > /dev/null 2>&1 || true
}
trap stop_tools EXIT

prom_value() {
  curl -fsS "http://localhost:3008/api/v1/query" --data-urlencode "query=$1" 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(int(float(r[0]["value"][1])) if r else 0)' 2>/dev/null \
    || echo 0
}

# all 16 partitions must be assigned, or the first experiments are skipped
echo "== Waiting for the consumers to get all partitions"
for _ in $(seq 60); do
  assigned=$(prom_value 'sum(consumer_assigned_partitions)')
  (( assigned >= 16 )) && break
  sleep 3
done
echo "   $assigned of 16 partitions assigned"

echo "== Starting the load generator (rates $LOADTEST_RATES req/s, $STAGE_SECONDS s each)"
"${compose[@]}" up -d --force-recreate --no-deps http-load-generator
for _ in $(seq 30); do
  "${compose[@]}" logs http-load-generator 2>&1 | grep -q "Post rebalance Assign" && break
  sleep 2
done

echo "== Starting the producer: $STAGES (experiments:req/s), $duration s"
date +%s.%N > "$LOADTEST_RUN_DIR/t0"
"${compose[@]}" up -d --force-recreate --no-deps experiment-producer

end=$(( $(date +%s) + duration + 15 ))
while (( $(date +%s) < end )); do
  left=$(( end - $(date +%s) ))
  printf '   %4d s left | lag %6s | API %6s req/s\n' "$left" \
    "$(prom_value 'sum(consumer_lag_messages)')" \
    "$(prom_value 'sum(rate(http_load_generator_response_count_total[30s]))')"
  sleep $(( left < 30 ? (left > 0 ? left : 1) : 30 ))
done

stop_tools
trap - EXIT

echo "== Report"
python3 loadtest/report.py "$LOADTEST_RUN_DIR"
echo "Saved in $LOADTEST_RUN_DIR. The stack still runs on $LOADTEST_TOPIC; 'docker compose up -d' goes back to the demo setup."

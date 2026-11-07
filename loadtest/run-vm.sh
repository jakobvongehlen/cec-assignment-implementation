#!/usr/bin/env bash
# The load test of run.sh against the group VM. The system under test, the
# producer and the notifications service run on the VM; the HTTP load generator
# runs here and queries the API over the internet.
#
# Usage: loadtest/run-vm.sh [stages]
# Settings (environment): VM, VM_KEY, VM_DIR, STAGE_SECONDS, EXPERIMENT_SECONDS,
#   SENSORS, LOADTEST_DOCUMENT_WAIT, LOADTEST_MAX_IN_FLIGHT
# Stop other stacks on the VM that read group11 first. The test stack
# (~/cec-loadtest) keeps running afterwards; the output says how to remove it.
set -euo pipefail
cd "$(dirname "$0")/.."

VM=${VM:-ubuntu@34.243.38.239}
VM_KEY=${VM_KEY:-auth/group11_rsa}
VM_DIR=${VM_DIR:-cec-loadtest}
STAGES=${1:-${STAGES:-100:500,200:1000,400:2000,600:3000,800:4000,1200:6000}}
STAGE_SECONDS=${STAGE_SECONDS:-90}
EXPERIMENT_SECONDS=${EXPERIMENT_SECONDS:-30}
SENSORS=${SENSORS:-2}
export LOADTEST_TOPIC=group11
export LOADTEST_DOCUMENT_WAIT=${LOADTEST_DOCUMENT_WAIT:-10}
# Connections to the API. More than 200 don't get more requests through the
# dev machine's connection (requests take 70-230 ms there, see README.md).
export LOADTEST_MAX_IN_FLIGHT=${LOADTEST_MAX_IN_FLIGHT:-200}
export LOADTEST_STAGE_SECONDS=$STAGE_SECONDS
host=${VM#*@}

ssh_vm() { ssh -i "$VM_KEY" -o IdentitiesOnly=yes -o ServerAliveInterval=30 "$VM" "$@"; }

run=vm-$(date +%Y%m%d-%H%M%S)
export LOADTEST_RUN_DIR=./loadtest/out/$run
export LOADTEST_GROUP=loadtest-http-$run
mkdir -p "$LOADTEST_RUN_DIR"
python3 loadtest/plan.py --stages "$STAGES" --stage-seconds "$STAGE_SECONDS" \
  --experiment-seconds "$EXPERIMENT_SECONDS" --sensors "$SENSORS" \
  --document-wait "$LOADTEST_DOCUMENT_WAIT" \
  --producer-config "$LOADTEST_RUN_DIR/producer-config.json" --plan "$LOADTEST_RUN_DIR/plan.json"
plan_value() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$LOADTEST_RUN_DIR/plan.json" "$1"; }
export LOADTEST_RATES=$(plan_value rate_schedule)
duration=$(plan_value duration)
cat > "$LOADTEST_RUN_DIR/hosts.json" <<EOF
[{"host_name": "group11-vm", "base_url": "http://$host:3003"}]
EOF
export LOADTEST_HOSTS_FILE=$LOADTEST_RUN_DIR/hosts.json

local_compose=(docker compose -f compose.yml -f loadtest/compose.yml)
remote_env="SUT_CPUS=0,1 TOOL_CPUS=0,1 LOADTEST_TOPIC=group11 LOADTEST_RUN_DIR=./loadtest/out/$run"
remote_compose="cd ~/$VM_DIR && $remote_env docker compose -p cec-loadtest -f compose.yml -f loadtest/compose.yml -f loadtest/compose.vm.yml"

echo "== Copying the repository to $VM:$VM_DIR"
{ git ls-files; git ls-files --others --exclude-standard; } | grep -v '^loadtest/out/' | sort -u \
  | tar -czf - -T - | ssh_vm "mkdir -p $VM_DIR && tar -xzf - -C $VM_DIR"
ssh_vm "mkdir -p $VM_DIR/loadtest/out/$run && ln -sfn ~/cec-assignment-implementation/auth $VM_DIR/auth && mkdir -p $VM_DIR/logs"
ssh_vm "cat > $VM_DIR/loadtest/out/$run/producer-config.json" < "$LOADTEST_RUN_DIR/producer-config.json"

echo "== Building the load tools here"
"${local_compose[@]}" --profile loadtest build experiment-producer http-load-generator
if ! ssh_vm "docker image inspect cec-loadtest-producer --format '{{.Id}}'" 2>/dev/null \
    | grep -qF "$(docker image inspect cec-loadtest-producer --format '{{.Id}}')"; then
  echo "== Copying the producer image to the VM"
  docker save cec-loadtest-producer | gzip | ssh_vm "gunzip | docker load"
fi

# the local consumers would join the same consumer group as the VM's, with the
# same static member ids (consumer-1, ...)
echo "== Stopping the local system under test (the local Prometheus keeps the load generator's metrics)"
"${local_compose[@]}" stop $("${local_compose[@]}" config --services | grep -vx prometheus) > /dev/null 2>&1 || true
"${local_compose[@]}" up -d --no-deps prometheus

echo "== Starting the system under test on the VM"
ssh_vm "$remote_compose up -d --no-build"

vm_prom() {
  ssh_vm "curl -fsS http://localhost:3008/api/v1/query --data-urlencode 'query=$1'" 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(round(float(r[0]["value"][1]), 2) if r else 0)' 2>/dev/null \
    || echo 0
}
local_prom() {
  curl -fsS "http://localhost:3008/api/v1/query" --data-urlencode "query=$1" 2>/dev/null \
    | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(int(float(r[0]["value"][1])) if r else 0)' 2>/dev/null \
    || echo 0
}

echo "== Waiting for the consumers to get all partitions"
for _ in $(seq 60); do
  assigned=$(vm_prom 'sum(consumer_assigned_partitions)')
  (( ${assigned%.*} >= 16 )) && break
  sleep 3
done
echo "   $assigned of 16 partitions assigned"

stop_tools() {
  "${local_compose[@]}" logs --no-color http-load-generator > "$LOADTEST_RUN_DIR/http-load-generator.log" 2>&1 || true
  "${local_compose[@]}" stop http-load-generator > /dev/null 2>&1 || true
  ssh_vm "$remote_compose stop experiment-producer >/dev/null 2>&1; $remote_compose logs --no-color experiment-producer" \
    > "$LOADTEST_RUN_DIR/experiment-producer.log" 2>&1 || true
}
trap stop_tools EXIT

echo "== Starting the load generator here (rates $LOADTEST_RATES req/s against http://$host:3003)"
"${local_compose[@]}" up -d --force-recreate --no-deps http-load-generator
for _ in $(seq 30); do
  "${local_compose[@]}" logs http-load-generator 2>&1 | grep -q "Post rebalance Assign" && break
  sleep 2
done

echo "== Starting the producer on the VM: $STAGES (experiments:req/s), $duration s"
# the VM's clock: the consumer windows are measured in the VM's Prometheus
ssh_vm "date +%s.%N; $remote_compose up -d --force-recreate --no-deps experiment-producer >/dev/null 2>&1" \
  | head -1 > "$LOADTEST_RUN_DIR/t0"

end=$(( $(date +%s) + duration + 15 ))
while (( $(date +%s) < end )); do
  left=$(( end - $(date +%s) ))
  printf '   %4d s left | lag %6s | VM busy %4s | API %6s req/s\n' "$left" \
    "$(vm_prom 'sum(consumer_lag_messages)')" \
    "$(vm_prom '1 - avg(rate(node_cpu_seconds_total{mode="idle"}[30s]))')" \
    "$(local_prom 'sum(rate(http_load_generator_response_count_total[30s]))')"
  sleep $(( left < 30 ? (left > 0 ? left : 1) : 30 ))
done

stop_tools
trap - EXIT

echo "== Report"
# the VM's Prometheus through a tunnel, its port is not open
ssh -i "$VM_KEY" -o IdentitiesOnly=yes -o ExitOnForwardFailure=yes -f -N -L 13008:localhost:3008 "$VM"
trap 'pkill -f "13008:localhost:3008" || true' EXIT
python3 loadtest/report.py "$LOADTEST_RUN_DIR" --prometheus http://localhost:13008 --http-prometheus http://localhost:3008
echo "Saved in $LOADTEST_RUN_DIR."
echo "The test stack still runs on the VM: ssh in and run"
echo "  cd $VM_DIR && docker compose -p cec-loadtest -f compose.yml -f loadtest/compose.yml -f loadtest/compose.vm.yml down"

#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-$(pwd)}"
RESULTS_FILE="$(mktemp)"
TMP_DIR="$(mktemp -d)"
trap 'rm -f "$RESULTS_FILE"; rm -rf "$TMP_DIR"' EXIT

if [[ ! -f "$PROJECT_DIR/compose.yml" ]]; then
  echo "compose.yml not found in $PROJECT_DIR" >&2
  exit 1
fi

services=( $(docker compose -f "$PROJECT_DIR/compose.yml" config --services 2>/dev/null | grep '^consumer_' | sort || true) )

if [[ ${#services[@]} -eq 0 ]]; then
  echo "No consumer_* services were found in $PROJECT_DIR/compose.yml" >&2
  exit 1
fi

echo "Collecting final SLA summaries from ${#services[@]} consumer services..."

for service in "${services[@]}"; do
  cid=$(docker compose -f "$PROJECT_DIR/compose.yml" ps -q "$service" 2>/dev/null || true)
  if [[ -z "$cid" ]]; then
    echo "Skipping $service: container is not running"
    continue
  fi

  docker update --restart=no "$cid" >/dev/null 2>&1 || true

  log_file="$TMP_DIR/${service}.log"
  echo "Sending Ctrl+C to $service"

  # Attach to the container and let SIGINT reach the Python app.
  (docker attach --sig-proxy=false "$cid" >"$log_file" 2>&1 || true) &
  attach_pid=$!
  sleep 1
  docker kill --signal=INT "$cid" >/dev/null 2>&1 || true
  wait "$attach_pid" || true

  metric_line=$(grep -E '=== Notification SLA Metrics ===' "$log_file" | tail -n 1 || true)

  if [[ -z "$metric_line" ]]; then
    metric_line=$(docker logs "$cid" 2>&1 | grep -E '=== Notification SLA Metrics ===' | tail -n 1 || true)
  fi

  if [[ -n "$metric_line" ]]; then
    printf '%s\n' "$service: $metric_line" >> "$RESULTS_FILE"
  else
    printf '%s\n' "$service: no final SLA summary found" >> "$RESULTS_FILE"
  fi

done

if [[ ! -s "$RESULTS_FILE" ]]; then
  echo "No SLA summary lines were captured. Make sure the consumers produced notifications before this run."
  exit 0
fi

python3 - "$RESULTS_FILE" <<'PY'
import re
import sys

path = sys.argv[1]
pattern = re.compile(
    r"=== Notification SLA Metrics === Sent: (\d+) \| Avg Latency: ([0-9.]+)s \| Min: ([0-9.]+)s \| Max: ([0-9.]+)s \| SLA Violations \(>=10s\): (\d+)",
)

total_sent = 0
total_latency = 0.0
total_violations = 0
min_latency = float('inf')
max_latency = 0.0
lines = []

with open(path, 'r', encoding='utf-8') as fh:
    for raw in fh:
        line = raw.strip()
        lines.append(line)
        m = pattern.search(line)
        if not m:
            continue
        sent = int(m.group(1))
        avg = float(m.group(2))
        mn = float(m.group(3))
        mx = float(m.group(4))
        violations = int(m.group(5))
        total_sent += sent
        total_latency += avg * sent
        total_violations += violations
        min_latency = min(min_latency, mn)
        max_latency = max(max_latency, mx)

print("=== Aggregated Notification SLA Metrics ===")
print(f"Total Sent: {total_sent}")
if total_sent > 0:
    print(f"Average Latency: {total_latency / total_sent:.3f}s")
    print(f"Min Latency: {min_latency:.3f}s")
    print(f"Max Latency: {max_latency:.3f}s")
else:
    print("Average Latency: 0.000s")
    print("Min Latency: 0.000s")
    print("Max Latency: 0.000s")
print(f"Total SLA Violations (>=10s): {total_violations}")
print()
print("Per-container results:")
for line in lines:
    print(line)
PY

echo
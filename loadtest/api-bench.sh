#!/usr/bin/env bash
# Throughput of the API on the machine it runs on, without the network in
# between, see README.md. Run it on the VM next to the test stack:
#   docker exec -i cec-loadtest-mypg-1 psql -U postgres -d temp_db -At < api-urls.sql | shuf > urls.txt
#   ./api-bench.sh urls.txt 2000 4000 8000 0      (0 = as fast as possible)
# Prints per rate: achieved req/s, latency p50/p99, success rate and the CPU
# (cores) of the API container and of oha itself.
set -euo pipefail
urls=$1; shift
api_cgroup=/sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' cec-loadtest-api-1).scope/cpu.stat
usage() { awk '/^usage_usec/ {print $2}' "$api_cgroup"; }
printf '%8s %10s %9s %9s %8s %9s %9s\n' target achieved p50_ms p99_ms success api_cores oha_cores
for rate in "$@"; do
  limit=(); (( rate > 0 )) && limit=(-q "$rate")
  before=$(usage); start=$(date +%s.%N)
  out=$( { /usr/bin/time -f '%U %S' ./oha -z 20s -c 100 "${limit[@]}" --no-tui --urls-from-file "$urls" --output-format json; } 2>/tmp/oha-time)
  elapsed=$(echo "$(date +%s.%N) - $start" | bc); after=$(usage)
  read -r user sys < /tmp/oha-time
  python3 - "$rate" "$elapsed" "$before" "$after" "$user" "$sys" <<PY
import json, sys
rate, elapsed, before, after, user, sys_ = sys.argv[1:]
d = json.loads('''$out''')
s, p = d["summary"], d["latencyPercentiles"]
elapsed = float(elapsed)
print(f"{rate or 'max':>8} {s['requestsPerSec']:>10.0f} {p['p50']*1000:>9.1f} {p['p99']*1000:>9.1f} "
      f"{s['successRate']:>8.3f} {(int(after)-int(before))/1e6/elapsed:>9.2f} {(float(user)+float(sys_))/elapsed:>9.2f}")
PY
done

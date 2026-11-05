#!/usr/bin/env bash
# Keeps a load test run in git: copies loadtest/out/<run> to loadtest/results/<run>
# with the report, the raw numbers, the plan and the logs. Logs are compressed
# (xz), without color codes and with lines cut at 400 characters (a producer
# that can't deliver a message dumps it as bytes, a few hundred MB per run).
# producer-config.json is left out: plan.py makes it again from plan.json.
#
# Usage: loadtest/keep.sh <run> [<run> ...]   (directory names in loadtest/out)
set -euo pipefail
cd "$(dirname "$0")"
for run in "$@"; do
  src=out/$run dst=results/$run
  [[ -d $src ]] || { echo "no such run: $src" >&2; exit 1; }
  mkdir -p "$dst"
  for f in report.md results.json plan.json t0 hosts.json; do
    [[ -f $src/$f ]] && cp "$src/$f" "$dst/"
  done
  for log in "$src"/*.log; do
    [[ -f $log ]] || continue
    sed 's/\x1b\[[0-9;]*m//g' "$log" | cut -c1-400 | xz -9 > "$dst/$(basename "$log").xz"
  done
  echo "$dst: $(du -sh "$dst" | cut -f1)"
done

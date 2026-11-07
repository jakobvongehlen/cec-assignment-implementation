#!/usr/bin/env bash
# Copies loadtest/out/<run> to loadtest/results/<run> to keep it in git: report,
# numbers, plan and logs (xz, no colors, lines cut at 400 characters).
# Usage: loadtest/keep.sh <run> [<run> ...]
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

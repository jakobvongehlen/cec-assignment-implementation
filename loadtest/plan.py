#!/usr/bin/env python3
"""Turns a list of load stages into the experiment-producer config and the
rate schedule of the http-load-generator, see README.md.

A stage is `<experiments>:<requests per second>`: that many experiments run at
the same time while the API gets that many requests per second. Experiments are
started one after another (every experiment-seconds / experiments seconds), so
the number running stays constant and finished experiments, which the load
generator queries, keep coming in.

Timeline, relative to the start of the producer (t = 0):
  0 .. warmup          stage 1 experiments only, so finished experiments exist
                       before the load generator starts querying
  warmup + (k-1)*S ..  stage k: its experiments and its request rate
  warmup + k*S
The first experiments of a stage replace those of the previous one during its
first experiment-seconds, so a stage is steady after that (its `steady` window).
"""
import argparse
import json
import math
import sys


def parse_stages(text):
    stages = []
    for part in text.split(","):
        experiments, rate = part.split(":")
        stages.append({"experiments": int(experiments), "rate": int(rate)})
    return stages


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stages", required=True, help="e.g. 50:100,100:250,200:500")
    parser.add_argument("--stage-seconds", type=int, default=120)
    parser.add_argument("--experiment-seconds", type=int, default=30,
                        help="length of one experiment (stabilization + carry-out, 1 sample per second)")
    parser.add_argument("--stabilization-samples", type=int, default=5)
    parser.add_argument("--sensors", type=int, default=2)
    parser.add_argument("--document-wait", type=int, default=10,
                        help="seconds the load generator waits before it queries a finished experiment")
    parser.add_argument("--producer-config", required=True)
    parser.add_argument("--plan", required=True)
    args = parser.parse_args()

    stages = parse_stages(args.stages)
    length = args.experiment_seconds
    carry_out = length - args.stabilization_samples
    if carry_out < 1:
        sys.exit("--experiment-seconds must be larger than --stabilization-samples")
    if args.stage_seconds <= length:
        sys.exit("--stage-seconds must be larger than --experiment-seconds, or a stage is never steady")

    # the load generator starts with the first finished experiment, which ends
    # after one experiment length, and then waits document-wait seconds
    warmup = length + args.document_wait + 2

    entries = []
    windows = []
    for k, stage in enumerate(stages):
        start = 0 if k == 0 else warmup + k * args.stage_seconds
        end = warmup + (k + 1) * args.stage_seconds
        # one experiment ends every length / experiments seconds, and another one starts
        per_second = stage["experiments"] / length
        for t in range(start, end):
            count = math.floor((t - start + 1) * per_second) - math.floor((t - start) * per_second)
            for _ in range(count):
                entries.append({
                    "start_time": t,
                    "researcher": "loadtest@group11",
                    "num_sensors": args.sensors,
                    "sample_rate": 1000,
                    "stabilization_samples": args.stabilization_samples,
                    "carry_out_samples": carry_out,
                    "start_temperature": 16,
                    "temp_range": {"lower_threshold": 25.5, "upper_threshold": 26.5},
                })
        http_start = warmup + k * args.stage_seconds
        windows.append({
            "stage": k + 1,
            "experiments": stage["experiments"],
            "rate": stage["rate"],
            "start": http_start,
            "end": end,
            # previous stage's experiments are gone, the stage is steady
            "steady_start": http_start if k == 0 else http_start + length,
        })

    with open(args.producer_config, "w") as f:
        json.dump(entries, f)
    plan = {
        "stage_seconds": args.stage_seconds,
        "experiment_seconds": length,
        "sensors": args.sensors,
        "warmup": warmup,
        # the experiments of the last stage still have to end
        "duration": warmup + len(stages) * args.stage_seconds + length,
        "experiments_total": len(entries),
        "rate_schedule": ",".join(str(s["rate"]) for s in stages),
        "stages": windows,
    }
    with open(args.plan, "w") as f:
        json.dump(plan, f, indent=2)
    print(f"{len(entries)} experiments, {len(stages)} stages, {plan['duration']} s")


if __name__ == "__main__":
    main()

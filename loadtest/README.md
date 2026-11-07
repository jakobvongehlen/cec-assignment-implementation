# Load test

Puts the consumers and the HTTP API under load at the same time, in stages. A stage is `<experiments>:<requests per second>`.

```
loadtest/run.sh 50:250,100:500,200:1000     # locally, system pinned to 2 cores
loadtest/run-vm.sh 450:100,500:150           # against the group VM
STAGE_SECONDS=120 EXPERIMENT_SECONDS=60 loadtest/run.sh 100:500
```

Each run prints a report and writes it with the raw numbers to `loadtest/out/<run>/`, which is not in git. `loadtest/keep.sh <run>` copies a run to [`results/`](results/README.md).

## Setup

| | Runs on | |
|---|---|---|
| System under test | `SUT_CPUS`, 2 cores | `../compose.yml`: consumers, API, Postgres, monitoring |
| `experiment-producer` | `TOOL_CPUS` | course producer, built from `../experiment-producer` |
| `http-load-generator` | `TOOL_CPUS` | course load generator, built from `../http-load-generator` |
| `notifications-service` | `TOOL_CPUS` | course service with our producer's key |

Experiments have 2 sensors at 1 sample/s and last `EXPERIMENT_SECONDS` (30). The load generator queries finished experiments, half `/temperature/out-of-range` and half `/temperature`, and checks every response. Everything uses the `group11` topic, the only one our client can write to.

`run-vm.sh` copies the repo to `~/cec-loadtest` on the VM and runs the stack, the producer and the notifications service there. The load generator runs locally and queries the VM over the internet.

Changes to the course tools:
- `experiment-producer` uses one Kafka client for all experiments in `--config-file` mode instead of one each.
- `http-load-generator` takes a `--rate-schedule` per stage, skips messages that aren't experiment documents and sends queued batches before it exits.

## Pass criteria

A stage passes if no notification takes 10 s or more or fails, and the API answers at least 95% of the requested rate with at most 1% errors. Wrong answers count as errors. The consumers are measured once the previous stage's experiments have ended. The report warns if the producer delivered less than 80% of the expected events, because lost messages then show up as wrong answers.

## Results on the VM (2026-11-05)

| Experiments | Events/s | Notification p50 / p99 | > 10 s | Max lag | VM busy |
|---|---|---|---|---|---|
| 100 | 216 | 0.17 / 0.38 s | 0 | 2 | 39% |
| 200 | 432 | 0.19 / 0.49 s | 0 | 22 | 51% |
| 400 | 870 | 0.20 / 0.82 s | 0 | 146 | 87% |
| **450** | **973** | **0.21 / 0.91 s** | **0** | **328** | **94%** |
| 500 | 1,053 | 2.8 / 14 s | 314 | 4,945 | 100% |
| 550 | 990 of 1,100 | 16 / 53 s | 4,180 | 26,558 | 100% |

The limit is CPU at about 1.4 ms per event: 1.1 ms in the consumer, 0.3 ms in Postgres. The producer and the notifications service took about 0.3 cores of the VM, so without them the limit would be around 500 to 550 experiments (estimate).

API: over the internet the dev machine's connection capped the load at about 1,000 to 1,400 req/s, with notifications at p99 0.4 s or less. On the VM itself (`api-bench.sh`) it served 10,385 req/s with p99 of 28 ms on 0.51 cores ([details](results/vm-20261105-api.md)).

The first VM run found that the API stopped answering at about 1,000 client connections. It reached Docker's default limit of 1,024 open files, and requests waited forever for a database connection. Fixed by raising the limit to 65,536 and adding `DB_TIMEOUT`. `fd-repro.py` reproduces it.

## Limits of the setup

- The producer and the notifications service share the VM's CPU with the system. In the demo they run elsewhere.
- From the dev machine the producer sends at most about 650 events/s and the HTTP load reaches about 1,400 req/s, limited by the connection.
- The VM had CPU credits during all runs. Without them it is throttled to 0.4 cores in standard mode, or pays extra in unlimited mode.

## Cleanup

`docker compose -f compose.yml -f loadtest/compose.yml down` stops the local test stack. `docker compose up -d` starts the normal stack again.

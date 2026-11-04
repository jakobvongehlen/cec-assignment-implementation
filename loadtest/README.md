# Load test: consumers and HTTP API at the same time

Finds the load at which the system stops meeting its requirements, with the
consumers and the REST API under load at the same time. The load goes up in
stages. Each stage is a number of experiments running at once (consumer
load) plus a request rate to the API (HTTP load):

```
loadtest/run.sh                                  # default stages, see below
loadtest/run.sh 50:250,100:500,200:1000          # <experiments>:<req/s>,...
STAGE_SECONDS=120 EXPERIMENT_SECONDS=60 loadtest/run.sh 100:500,300:1500
```

The run takes `warmup + stages × STAGE_SECONDS` (about 10 min for the default
stages). At the end it prints a report and saves it with the raw numbers in
`loadtest/out/<run>/` (`report.md`, `results.json`, the producer config, the
tool logs).

## What runs

`run.sh` starts the stack of `../compose.yml` with the overlay `compose.yml`:

| | Where | What |
|---|---|---|
| System under test | cores `SUT_CPUS` (default `0,1`) | the 16 consumers, the API, Postgres and the monitoring, like on the group VM (t3a.medium, 2 vCPUs) |
| `experiment-producer` | cores `TOOL_CPUS` (default 2..n) | the course producer (built from `../experiment-producer`), runs the experiments of the stages |
| `http-load-generator` | `TOOL_CPUS` | the course HTTP load generator (built from `../http-load-generator`), queries the API |
| `notifications-service` | `TOOL_CPUS` | the course notifications service with our producer's key; the consumers notify this one |

Both load tools are the course's, so the load has the same shape as the demo:

- **Consumers**: experiments with 2 sensors at 1 sample/s (the requirement's
  sampling rate), each `EXPERIMENT_SECONDS` long (default 30, 5 of them
  stabilization). They start one after another, so the number running at once
  stays at the stage's value. Notifications go to the local notifications
  service, which reports the latency from the measurement to the notification;
  the consumers record it (`notification_latency_seconds`).
- **HTTP API**: the load generator only queries **finished** experiments: the
  producer writes an `experiment_document` with all measurements when an
  experiment ends, the load generator waits `LOADTEST_DOCUMENT_WAIT` (10 s)
  and from the next stage on picks random experiments from all finished ones.
  Half the queries are `/temperature/out-of-range`, half `/temperature` with
  a random interval. Every response is compared with the document, so
  wrong answers count as errors as well. `LOADTEST_MAX_IN_FLIGHT` (200)
  requests are in flight at most.

Everything uses the `group11` topic (`LOADTEST_TOPIC`), the only one our Kafka
client may write to. The experiment documents go to the same topic, because
the client can't write to `experiment-document`. The load generator skips the
experiment events (they have a `record_name` header, the documents don't), and
the consumers ignore the documents.

## Changes to the course tools

- `experiment-producer`: in `--config-file` mode all experiments share one
  Kafka client instead of one each. With thousands of experiments, one client
  each meant thousands of threads and broker connections.
- `http-load-generator`: `--rate-schedule 250,500,...` sets the rate of each
  generation (stage) instead of a random one between the batch sizes. It
  skips messages that are not experiment documents instead of crashing, and
  sends the batches still queued after the last generation before it exits.

## Report

The consumer side of a stage is measured over its steady window: from when
the experiments of the previous stage have ended (`EXPERIMENT_SECONDS` into the
stage) to its end. The API side is measured while the load generator really
sent the stage's rate (from its `http_load_generator_target_request_rate`
gauge), because its stages drift a few seconds from the plan. Per stage:

- **Consumers**: events consumed per second (expected: experiments × sensors),
  the largest consumer lag, processing time per message, notifications and
  their latency (p50/p99), how many took more than 10 s, failed notifications.
- **API**: requested and achieved request rate, round-trip time (p50/p99) of
  correct responses, errors by type (`ServerError`: no response or a
  connection error, `ValidationError`: a wrong answer, ...).
- **CPU** of the system under test per part (from cAdvisor) in cores, out of 2.

A stage passes if no notification took 10 s or more and none failed, and the
API answered at least 95 % of the requested rate with at most 1 % errors. The
limit is the last stage that passed.

**Check that the producer delivered the load** before reading a failed stage
as a limit: the report warns when the consumers got less than 80 % of the
expected events without falling behind. The producer gives up on a message
after 5 s (`Failed to produce message ... MessageTimedOut` in `tools.log`), and
every lost reading leaves a measurement incomplete. The consumers then never
store it, and the API's answers miss it. The load generator counts these as
`ValidationError`s. The notifications service rejects notifications whose
previous measurement went missing (`400: Unexpected notification`). These
errors come from the load source, not from the system under test.

While it runs, the dashboard at http://localhost:3010 shows the same, and
Prometheus at http://localhost:3008 has the load generator's metrics as well
(`http_load_generator_*`).

## On the group VM: `run-vm.sh`

```
loadtest/run-vm.sh                                   # default stages
STAGE_SECONDS=120 loadtest/run-vm.sh 450:100,500:150  # <experiments>:<req/s>,...
```

Runs the same test against the group VM (`VM`, default
`ubuntu@34.243.38.239`, key `auth/group11_rsa`):

- It copies the repository to `~/cec-loadtest` on the VM and starts the stack
  there as compose project `cec-loadtest`, with `compose.vm.yml` on top.
  `compose.vm.yml` uses the consumer image already on the VM, with the current
  `single_consumer.py`/`notifier.py` mounted in (the VM disk is too full to
  build one), and caps the container logs.
- The **producer and the notifications service run on the VM**: from the dev
  machine the producer can't send more than ~650 events/s (see the results
  below). They take CPU from the system; the report shows how much ("Load
  tools").
- The **HTTP load generator runs on this machine** and queries
  `http://<vm>:3003` over the internet, like the course's. Its metrics go to
  the local Prometheus; the report reads the VM's Prometheus through an SSH
  tunnel.
- Anything else on the VM that reads `group11` has to be stopped first.

`api-bench.sh` measures the API on the VM without the network in between, with
[`oha`](https://github.com/hatoo/oha) and the URLs of `api-urls.sql` (the
comments in it say how).

## Limits of the setup

- The producer and the notifications service share the VM's 2 vCPUs with the
  system (0.25–0.35 cores at 450–550 experiments). In the demo they run
  elsewhere, so the VM handles somewhat more than measured.
- HTTP load from the dev machine is limited by its connection: requests take
  70–230 ms, so 200 connections give at most ~1,000–1,400 req/s. The API's own
  limit comes from `api-bench.sh`.
- The VM is a t3a.medium: it earns CPU credits for 20 % per vCPU (0.4 cores)
  and spends them above that. All runs had credits (steal ≤ 4 %). Without
  credits in standard mode it is throttled to 0.4 cores. In unlimited mode the
  extra CPU costs extra.
- Experiments only become queryable at the start of the next stage (the load
  generator adds new experiments once per generation), so the API always
  serves terminated experiments, from its cache when possible.

## Results on the group VM (2026-11-05)

t3a.medium, 2 vCPUs; experiments with 2 sensors at 1 sample/s, 30 s each.

**Consumers** (`run-vm.sh`, light HTTP load):

| Experiments | Events/s | Notification p50 / p99 | > 10 s | Max lag | VM busy | Consumers + Postgres | Producer etc. |
|---|---|---|---|---|---|---|---|
| 100 | 216 | 0.17 / 0.38 s | 0 | 2 | 39 % | 0.43 cores | 0.08 |
| 200 | 432 | 0.19 / 0.49 s | 0 | 22 | 51 % | 0.70 | 0.11 |
| 400 | 870 | 0.20 / 0.82 s | 0 | 146 | 87 % | 1.23 | 0.25 |
| **450** | **973** | **0.21 / 0.91 s** | **0** | **328** | **94 %** | **1.34** | **0.27** |
| 500 | 1,053 | 2.8 / 14 s | 314 | 4,945 | 100 % | 1.40 | 0.31 |
| 550 | 990 (of 1,100) | 16 / 53 s | 4,180 | 26,558 | 100 % | 1.37 | 0.35 |
| 600 | 1,001 (of 1,200) | 7.5 / 29 s | 1,501 | 17,729 | 100 % | 1.36 | 0.35 |

The consumers handle **450 experiments (~1,000 events/s)**; at 500 the VM's
CPU is used up, the lag grows and notifications come late. The limit is CPU:
~1.4 ms per event (consumers 1.1 ms, Postgres 0.3 ms). Without the producer
and the notifications service on the VM (~0.3 cores) that would be about
**500–550 experiments**, an estimate. The requirement (100 experiments) uses
~40 % of the VM, so there is about 4.5× headroom.

**API**:

- Over the internet (`run-vm.sh 100:250,...,100:4000`, 100 experiments
  running): all answers correct; the rate stops at ~1,000–1,400 req/s, the
  limit of the dev machine's connection (the API used 0.14 cores, the VM 46 %).
  Notifications stayed at p99 ≤ 0.4 s during all of it.
- On the VM (`api-bench.sh`, consumers idle):

  | req/s | p50 | p99 | API | oha |
  |---|---|---|---|---|
  | 2,000 | 0.5 ms | 5.8 ms | 0.19 cores | 0.22 |
  | 4,000 | 0.6 ms | 17 ms | 0.27 | 0.27 |
  | 8,000 | 3.3 ms | 28 ms | 0.44 | 0.45 |
  | max: 10,385 | 8.6 ms | 28 ms | 0.51 | 0.48 |

  ~0.055 ms CPU per request; 1,000 req/s cost about as much CPU as 40
  events/s (20 experiments).

**Found: the API hangs for good at ~1,000 client connections.** The API
container may open 1,024 files (Docker's default soft limit). With the load
generator at 1,000 connections in flight it reached 1,018 open sockets, after
which it accepted no connections (kernel: "Possible SYN flooding on port
3003"), couldn't connect to Postgres, and waited forever for a pool connection
(`pool.get()` has no timeout). It only came back after a restart. Fix: raise
the limit in `compose.yml` (`ulimits: nofile: 65536`) and give the pool a
wait timeout. `run-vm.sh` uses 200 connections for that reason.

## Results on the dev machine (2026-11-04)

`run.sh`, system pinned to 2 cores of the dev machine. 100 experiments with
500 req/s passed (notification p99 1.5 s, API p99 21 ms), and the API reached
8,000 req/s. Above ~300 experiments the producer couldn't send more than
~650 events/s: its upload to the course Kafka stays at ~0.9 MB/s, it lost
messages to timeouts (seen as `ValidationError`s and rejected notifications),
and a pool of 8 Kafka clients didn't help. The dev machine's cores are about
twice as fast per event as the VM's (0.7 vs 1.4 ms), so an extrapolation from
here overestimates the VM.

## Afterwards

The stack keeps running on `group11` and the local notifications service.
`docker compose -f compose.yml -f loadtest/compose.yml down` stops all of it.
`docker compose up -d` (without the overlay) puts the consumers back on the
demo topic; `docker compose -f compose.yml -f loadtest/compose.yml rm -sf
notifications-service` removes the notifications service.

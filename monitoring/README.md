# Monitoring

The monitoring does two jobs:

1. **Cost overview.** The assignment requires "a monitoring component that provides an overview of the microservice's costs over time".
2. **Input for the autoscaler.** The autoscaler (`src/autoscaler/autoscaler.py`) asks Prometheus how far behind the consumers are and adds or removes consumers based on the answer. Without Prometheus, scaling doesn't work.

It starts with the rest of the stack (`docker compose up -d --build`).

## How it works

```
 node-exporter ── VM: CPU, memory, network, disk ─┐
 cAdvisor ─────── CPU and memory per container ───┼──>  Prometheus  ──>  Grafana dashboard
 consumers ────── Kafka lag, time per message ────┘     (stores the       (:3010)
                                                         numbers, runs
                                                         cost.yml)    ──>  autoscaler
```

1. **Collect.** Three sources report numbers: node-exporter for the VM, cAdvisor for each container, and the consumers for their own work (Kafka lag, time per message, notifications).
2. **Store.** Prometheus pulls these numbers every 15 s (every 2 s from the consumers, because the autoscaler needs them quickly) and stores them.
3. **Compute the cost.** `prometheus/rules/cost.yml` multiplies usage by AWS prices. The result is a new metric: dollars per hour.
4. **Show.** Grafana draws the cost and the load on one dashboard.
5. **Scale.** The autoscaler reads the consumer lag and load from Prometheus every 5 s.

## Services

| Service | Port | What it does |
|---|---|---|
| `prometheus` | `3008` | Collects and stores all metrics, computes the cost. The course Prometheus also reads the VM CPU and memory from here for the course Grafana. |
| `node-exporter` | `9100` (host network) | VM CPU, memory, network and disk |
| `cadvisor` | internal | CPU, memory and network per container |
| `grafana` | `3010` | The "Temperature Observability" dashboard. Anyone can view it without logging in. Change it in `grafana/dashboard.py`, not in the UI. |

Each consumer serves its metrics on `:8000/metrics`. Prometheus finds the consumer containers on its own through the Docker socket, so it keeps working when the autoscaler adds or removes consumers.

## The cost estimate

Our VM is a t3a.medium in eu-west-1 (2 vCPU, 4 GiB, 20 GiB disk). Most of what AWS charges for it is fixed. Only two parts grow with load:

| Part | Price | Grows with load? |
|---|---|---|
| instance | $0.0408/h | no |
| public IPv4 | $0.005/h | no |
| disk (gp3) | $0.088 per GiB-month | no |
| CPU credits | $0.05 per vCPU-hour above 20% CPU per vCPU | yes |
| outbound traffic | $0.09 per GiB | yes |

Idle, that comes to about **$0.049/h, or $35/month**.

The CPU credits part needs one extra sentence. A t3a VM may use 20% of each vCPU for free. Anything above that is billed as CPU credits. The estimate ignores credits the VM has saved up, so it shows the worst case.

The metric is `cec:cost_usd_per_hour`, one series per part. The prices are at the top of `cost.yml` (AWS on-demand prices, checked October 2026). Update them if the VM type changes.

**Cost per container (showback).** This is an extra metric, `cec:container:cost_usd_per_hour`. It divides the instance price among the containers by how much CPU and memory each one uses, to show where the money goes. AWS doesn't bill per container, so this is only an attribution. To weigh CPU against memory, it uses the ratio of AWS Fargate's vCPU price to its GiB price. Whatever no container uses (idle capacity, the OS, Docker) is `cec:unused_cost_usd_per_hour`.

## The dashboard

| Row | Shows |
|---|---|
| Estimated cost | cost right now, cost over the selected time range, projected cost per month, cost per part and per container |
| Where is the bottleneck? | CPU and memory per container, Kafka lag, messages consumed, consumer time per message, VM CPU, memory and network |
| Scaling | number of consumers, how busy each one is, skipped messages |
| Notifications | notification latency (must stay under 10 s), notifications sent, waiting or failed |

## Finding the bottleneck

| Component | Look for |
|---|---|
| Consumer | CPU near 1.0 vCPU in *CPU per container*. A consumer is single-threaded Python, so it can't use more than one core. Growing Kafka lag means more consumers are needed (at most one per partition). |
| Postgres | *DB insert* takes most of the *Consumer time per message*, or `mypg` CPU is high. Each measurement is one INSERT. |
| Notifier | *Notifications waiting in the thread pool* grows, or the *Request time to notifications service* rises. The pool size is `NOTIFICATIONS_MAX_WORKERS`. |
| API | `api` CPU in *CPU per container*. Request rate and response times are in the course Grafana (from the HTTP load generator). |
| VM | *VM CPU* above the 20% line. From then on every hour costs CPU credits. |

---

## Appendix: overhead

Measured on the test stack (40 experiments with 1–4 sensors each, about 60 messages/s):

| | CPU | Memory |
|---|---|---|
| prometheus | 0.004 vCPU | 108 MiB |
| cadvisor | 0.004 vCPU | 19 MiB |
| node-exporter | 0.002 vCPU | 9 MiB |
| grafana (idle) | 0.001 vCPU | 59 MiB |
| **total** | **~0.011 vCPU** | **~195 MiB** |

That is about 1% of the VM's CPU and 5% of its memory. If the VM gets tight during the stress test, stop Grafana first. Prometheus keeps collecting without it. Prometheus keeps data for 30 days or 1 GB, whichever comes first, because the disk is small.

**Consumer metrics** add about 5 µs per message. Decoding a message takes about 100 µs and the DB insert about 2.5 ms, so the metrics cost under 1% of the consumer's time.

**The API has no metrics of its own.** We tried a request histogram in the Python API, and it cut throughput by about 11% on the path the HTTP stress test hits most (4,095 → 3,651 req/s). The course's HTTP load generator already measures request rate and latency, and cAdvisor shows the API's CPU.

## Appendix: setup notes

- **cAdvisor needs v0.57 or newer.** Docker 29 stores images in containerd, and older versions can't read that.
- **Prometheus runs as root** so it can read the Docker socket to find the consumers.
- **node-exporter uses the host network and PID namespace.** Otherwise it would measure its own container instead of the VM. Port 9100 is closed in the VM's security group, so it isn't public.

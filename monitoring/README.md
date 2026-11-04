# Monitoring

The monitoring gives an overview of the service's costs over time, as the assignment requires, the CPU each container uses and whether the consumers keep up with Kafka. Whether notifications arrive within 10 s is in the consumer logs (see below).

It starts with the rest of the stack (`docker compose up -d --build`).

## How it works

```
 node-exporter ── VM: CPU, memory, network, disk ─┐
 cAdvisor ─────── CPU per container ──────────────┼──>  Prometheus  ──>  Grafana dashboard
 consumers ────── Kafka lag ──────────────────────┘     (stores the       (:3010)
                                                         numbers, runs
                                                         cost.yml)
```

1. **Collect.** node-exporter measures the VM, cAdvisor each container, and each consumer reports how far it is behind in Kafka (lag).
2. **Store.** Prometheus pulls these numbers every 15 s and stores them.
3. **Compute the cost.** `prometheus/rules/cost.yml` multiplies the VM's usage by AWS prices. The result is a new metric: dollars per hour.
4. **Show.** Grafana draws the cost, the CPU per container and the lag on one dashboard.

## Services

| Service | Port | What it does |
|---|---|---|
| `prometheus` | `3008` | Collects and stores all metrics, computes the cost. The course Prometheus also reads the VM CPU and memory from here for the course Grafana. |
| `node-exporter` | `9100` (host network) | VM CPU, memory, network and disk |
| `cadvisor` | internal | CPU per container |
| `grafana` | `3010` | The "Temperature Observability" dashboard. Anyone can view it without logging in. Change it in `grafana/dashboard.py`, not in the UI. |

Prometheus scrapes the consumers' own metrics from `consumer_1:8000` to `consumer_16:8000`. The dashboard only shows the Kafka lag; the others (time per message, notifications, skipped messages) can be queried in Prometheus on `:3008`.

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

## The dashboard

| Row | Shows |
|---|---|
| Estimated cost | projected cost per month, cost per part over time |
| Load | CPU per container (the 16 consumers added up as `consumers`) and the VM's 2 vCPUs; Kafka consumer lag, the messages the consumers have not read yet |

## Notifications within 10 s: the logs

Each consumer logs every notification with its latency, and a summary when it stops:

```
=== Notification SLA Metrics === Sent: ... | ... | Max: ...s | SLA Violations (>=10s): 0
```

`scripts/aggregate_consumer_sla.sh` stops the 16 consumers and adds up their summaries. The notifications service itself also logs the latency of each notification (`docker logs notifications-service` for a local one).

## Finding the bottleneck

| Component | Look for |
|---|---|
| Consumers | Growing *Kafka consumer lag*: the consumers can't keep up, and notifications get later. Then check `consumers` CPU in *CPU per container*. A consumer is single-threaded Python, so 16 consumers can use at most the VM's 2 vCPUs. |
| Postgres | `mypg` CPU. Each measurement is one INSERT. |
| API | `api` CPU. Request rate and response times are in the course Grafana (from the HTTP load generator). |
| VM | Total CPU near the 2 vCPUs. Above 20% per vCPU every hour costs CPU credits, visible as `cpu_credits` in the cost. |

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
- **node-exporter uses the host network and PID namespace.** Otherwise it would measure its own container instead of the VM. Port 9100 is closed in the VM's security group, so it isn't public.

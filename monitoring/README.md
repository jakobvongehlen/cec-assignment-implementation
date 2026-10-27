# Monitoring

Cost and bottleneck monitoring for the Temperature Observability service. It starts with the rest of the stack (`docker compose up -d --build`).

| Service | Port | What it does |
|---|---|---|
| `prometheus` | `3008` | Scrapes everything below, computes the cost estimate. The course Prometheus federates `node_cpu_seconds_total` and `node_memory_*` from here for the CPU/memory panels in the course Grafana. |
| `node-exporter` | `9100` (host network) | VM CPU, memory, network, disk |
| `cadvisor` | internal | CPU, memory and network per container |
| `grafana` | `3010` | The "Temperature Observability" dashboard. It is public, so login is turned off completely: everyone is an anonymous viewer who can also use Explore. Change the dashboard in `grafana/dashboard.py`, not in the UI. |

The consumers serve their own metrics on `:8000/metrics` inside the compose network. Prometheus finds every container whose compose service is `consumer` or `consumer_<n>` through the Docker socket, so adding consumers needs no config change.

## Estimated cost

`prometheus/rules/cost.yml` turns usage into USD per hour. All prices are at the top of that file: AWS on-demand prices for eu-west-1, checked October 2026. Update them if the VM type changes.

**Billed cost** (`cec:cost_usd_per_hour`, one series per component) is what AWS charges for our VM, a t3a.medium with 2 vCPU, 4 GiB and a 20 GiB gp3 volume:

| Component | Price | Depends on load? |
|---|---|---|
| instance | $0.0408/h | no |
| public IPv4 | $0.005/h | no |
| storage | $0.088 per GiB-month, size from node_exporter | no |
| cpu_credits | $0.05 per vCPU-hour above the 20% per-vCPU baseline | yes |
| egress | $0.09/GiB sent out of the VM | yes |

That comes to about $0.049/h, or $35/month, when idle. T3a instances run in "unlimited" mode by default, so sustained CPU above the baseline is billed as surplus credits. The estimate ignores the credit balance the instance has earned, so it shows the worst case.

**Showback** (`cec:container:cost_usd_per_hour`) splits the instance price over the containers. Each container's share is based on the CPU and memory it uses, priced with Fargate's vCPU:GiB price ratio. This shows which part of the service the money goes to. `cec:unused_cost_usd_per_hour` is the capacity no container uses: idle time, the OS and dockerd.

The dashboard shows:
- the current cost per hour
- the cost over the selected time range
- a projection for the month
- the cost per component and per container over time

## Finding the bottleneck

The dashboard's middle and bottom rows are laid out for this. Typical signals:

- **Consumer:** CPU near 1.0 vCPU in *CPU per container*. The consumer is single-threaded Python, so it can't use more than one core.
  - Kafka consumer lag growing means more consumers are needed, up to the number of partitions.
- **Postgres:** *DB insert* is most of the *Consumer time per message*, or `mypg` CPU is high. Each measurement is one autocommitted INSERT.
- **Notifier:** *Notifications waiting in the thread pool* grows, or the *request time to notifications service* rises. The thread pool size is `NOTIFICATIONS_MAX_WORKERS`.
- **API:** `api` CPU in *CPU per container*. Request rate and response times come from the HTTP load generator, in the course Grafana.
- **VM:** *VM CPU* goes above the t3a baseline line (20%). It can stay there until the CPU credits run out, and every hour above it costs surplus credits.

## Overhead

Measured with the test stack on the development machine (40 experiments of 1–4 sensors, about 60 messages/s):

| | CPU | Memory |
|---|---|---|
| prometheus | 0.004 vCPU | 108 MiB |
| cadvisor | 0.004 vCPU | 19 MiB |
| node-exporter | 0.002 vCPU | 9 MiB |
| grafana (idle) | 0.001 vCPU | 59 MiB |
| **total** | **~0.011 vCPU** | **~195 MiB** |

On the t3a.medium that is about 1% of the CPU (roughly 5% of the 0.4 vCPU baseline) and 5% of the memory. If the VM gets tight during the stress test, Grafana is the easiest service to stop: Prometheus keeps collecting either way.

The Prometheus TSDB is capped at 30 days or 1 GB, because the VM disk is small.

**Consumer metrics** cost about 3.4 µs per message, plus 1.7 µs per stored measurement. Decoding one message takes about 100 µs, and the DB insert about 2.5 ms (p95), so the metrics are well under 1% of the consumer's time per message. The INFO line the consumer used to log for every averaged measurement cost about 18 µs, more than all the metrics together, and filled the Docker logs on a nearly full disk. It is now DEBUG.

**API metrics are deliberately not added.** A request histogram in the Python API, written as a pure ASGI middleware with multiprocess mode for the uvicorn workers, cut throughput on cached experiments by about 11% (4,095 → 3,651 and 3,488 → 3,090 req/s in two rounds, 64 connections). Cached experiments are the path the HTTP stress test hits most. Request rate and latency are already measured by the course's HTTP load generator and shown in the course Grafana, and cAdvisor shows the API's CPU.

## Notes

- **cAdvisor needs v0.57 or newer.** Docker 29 stores images in containerd, which older cAdvisor versions can't read: they only report the root cgroup.
- **Prometheus runs as root.** It needs that to read the Docker socket for finding the consumers.
- **node-exporter needs the host network and pid namespace.** Otherwise it would report its own container instead of the VM. Port 9100 is blocked by the VM's security group, so it is not public; Prometheus reaches it on the host.

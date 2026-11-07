# Temperature Observability, group 11

Python consumers read experiment events from Kafka, store averaged measurements in Postgres and notify researchers. A Rust HTTP API serves the stored measurements. Everything runs with Docker Compose on the group VM (`docker compose up -d --build`).

## Architecture

```mermaid
flowchart TB
    subgraph course[" Course · AWS eu-west-1 "]
        producer("Experiment producer")
        kafka[("Kafka<br/>topic experiment, 16 partitions")]
    end

    subgraph vm[" Group VM "]
        consumers("16 consumers<br/>Python, one per partition")
        db[("Postgres")]
        api("HTTP API<br/>Rust, caches finished experiments")
        mon("Monitoring<br/>Prometheus, Grafana, cAdvisor")
    end

    notify("Notifications service<br/>course")
    clients("HTTP clients<br/>load generator, researchers")

    producer -->|"1 event per sensor per second"| kafka
    kafka -->|"1 partition per consumer"| consumers
    consumers -->|"1 INSERT per measurement"| db
    consumers -->|"out of range, stabilized"| notify
    db -->|"finished experiments every 1 s,<br/>cache misses"| api
    api -->|"GET /temperature,<br/>/temperature/out-of-range"| clients
    consumers -.->|"metrics"| mon
```

## Consumers

## Notifications

## HTTP API

Rust service in [`src/http-api-rust`](src/http-api-rust) on port 3003, built with axum and tokio. It replaced our first version in FastAPI ([`src/http-api`](src/http-api)).

- Postgres builds the response JSON. The API passes it through without parsing.
- Finished experiments can't change, so the 8,000 most recently terminated ones are cached in memory and refreshed every second. Running experiments are read from Postgres.
- One process with 2 threads, sharing the cache and a pool of 20 database connections.
- A request may spend at most 5 s on the database and then returns 500. Without this limit, a stalled database left requests hanging until the API stopped accepting connections.

On the VM it served 10,385 req/s with p99 latency of 28 ms ([results](loadtest/results/vm-20261105-api.md)). Locally it handles 2.7 to 4 times the requests of the FastAPI version.

## Database

## Bottlenecks and learnings

The VM handles 450 concurrent experiments, about 1,000 events/s, with notification p99 under 1 s. The requirement is 100 ([load test](loadtest/README.md)).

| Bottleneck | Limit |
|---|---|
| VM CPU, 2 vCPU | 1.4 ms CPU per event: 1.1 ms in the consumer, 0.3 ms in Postgres. Reached first. |
| Kafka | 16 partitions, so at most 16 consumers in parallel |
| CPU credits | The instance includes 0.4 cores; 100 experiments use about 0.7 |

1. On 2 vCPUs, capacity depends on CPU per event, not on the number of consumers. More consumers only add rebalances.
2. Keeping experiment state in Postgres and committing only completed work lets a partition move to another consumer without losing events.
3. Every wait needs a limit. Without a database timeout, and with Docker's default open file limit, the API stopped accepting connections under load.

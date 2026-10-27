# Autoscaler

Scales the `consumer` service between `MIN_CONSUMERS` and `MAX_CONSUMERS` containers. It runs as a service in [compose.yml](../../compose.yml) and needs the Docker socket and the Prometheus from [monitoring](../../monitoring/README.md).

## How it decides

Every 15 s it reads from Prometheus:

| Signal | Query | Meaning |
|---|---|---|
| delay | Kafka lag / consume rate | Seconds the consumers are behind. Notifications must arrive within 10 s. |
| busy | `rate(consumer_message_processing_seconds_sum[1m])`, averaged | Share of time a consumer is processing. One consumer is one Python thread, so 1.0 is saturated. |
| partitions | `consumer_topic_partitions` | More consumers than partitions would sit idle. |
| VM CPU | `cec:node:vcpus_used / cec:node:vcpus` | When the VM is saturated, more consumers don't help. |

- **Up:** delay above 2 s or busy above 60%, in 2 checks in a row. It adds enough consumers to bring the busy share to 50%, at least one. Not above the partition count, and not when the VM CPU is above 90%; then the VM is the bottleneck, and it logs that.
- **Down:** one consumer less would still be below 40% busy, and the delay is below 0.5 s, in 8 checks in a row (2 minutes). It removes one consumer at a time.
- **Cooldown:** 60 s after every change, until the rebalance and the 1-minute rates have settled.

All thresholds are environment variables at the top of [autoscaler.py](autoscaler.py).

## How it scales

- **Adding a consumer:** it copies a running consumer container: same image, command, environment, mounts, network and compose labels with the next container number. `docker compose ps` lists it as one of the service's containers, and `docker compose down` removes it. Copying the running container also means a new consumer always gets the same topic, even if the stack was started with `CONSUMER_TOPIC` set.
- **Removing a consumer:** it stops the newest consumer with `SIGTERM`. The consumer commits its offsets and leaves the group before its partitions move. See [Scaling and Consistency](../consumer/README.md#scaling-and-consistency).

The decisions are in its log: `docker compose logs -f autoscaler`.

## Limits

- **The VM:** it has 2 vCPUs, and a consumer spends most of its time waiting for Postgres. More consumers help until Postgres or the CPU is saturated, and the dashboard shows which comes first.
- **Rebalancing:** every change pauses the partitions that move for a moment. Scaling down is therefore slow and one step at a time.

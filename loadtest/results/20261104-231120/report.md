# Load test 20261104-231120

2 stages of 45 s, experiments of 15 s with 2 sensors at 1 sample/s, 216 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers (Kafka → Postgres → notifications)

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 20 | 48 (40) | 2 | 6.3 ms | 171 | 0.18 s | 0.90 s | 0 | 0 | ✅ |
| 2 | 40 | 95 (80) | 0 | 5.0 ms | 244 | 0.20 s | 1.88 s | 0 | 0 | ✅ |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 45–70 | 300 | 317 | 2.7 ms | 12.7 ms | 0.00 % | – | ✅ |
| 2 | 85–115 | 600 | 621 | 3.0 ms | 18.8 ms | 0.00 % | – | ✅ |

## CPU of the system under test (cores)

Load tools: producer, notifications service and load generator, where they run on the same machine. Machine busy and steal: all CPUs of the machine (node exporter).

| Stage | consumers | api | postgres | monitoring | Total | Load tools | Machine busy | Steal | Memory |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.08 | 0.02 | 0.05 | 0.03 | 0.18 | 0.09 | -1 % | 0.0 % | 1,308 MB |
| 2 | 0.10 | 0.03 | 0.06 | 0.03 | 0.22 | 0.14 | 1 % | 0.0 % | 1,318 MB |

## Result

All stages passed, up to 40 experiments and 600 req/s. The limit is above this load.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

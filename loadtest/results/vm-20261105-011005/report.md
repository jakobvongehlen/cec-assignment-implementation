# Load test vm-20261105-011005

2 stages of 60 s, experiments of 30 s with 2 sensors at 1 sample/s, 540 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers (Kafka → Postgres → notifications)

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 217 (200) | 6 | 48.1 ms | 874 | 0.17 s | 0.25 s | 0 | 0 | ✅ |
| 2 | 100 | 216 (200) | 18 | 49.6 ms | 425 | 0.17 s | 0.28 s | 0 | 0 | ✅ |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 55–215 | 500 | 105 | 564.8 ms | 10,000.0 ms | 0.00 % | – | ❌ |
| 2 | 132–162 | 1000 | 0 | 0.0 ms | 0.0 ms | – | – | ❌ |

## CPU of the system under test (cores)

Load tools: producer, notifications service and load generator, where they run on the same machine. Machine busy and steal: all CPUs of the machine (node exporter).

| Stage | consumers | api | postgres | monitoring | Total | Load tools | Machine busy | Steal | Memory |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.29 | 0.03 | 0.12 | 0.04 | 0.48 | 0.07 | 34 % | 1.3 % | 857 MB |
| 2 | 0.32 | – | 0.13 | 0.03 | 0.48 | 0.06 | 35 % | 4.5 % | 861 MB |

## Result

No stage passed. Stage 1 (100 experiments, 500 req/s) failed on the API.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

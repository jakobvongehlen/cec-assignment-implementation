# Load test vm-20261105-001500

6 stages of 90 s, experiments of 30 s with 2 sensors at 1 sample/s, 1940 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers (Kafka → Postgres → notifications)

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 217 (200) | 29 | 48.8 ms | 1,286 | 0.17 s | 0.25 s | 0 | 0 | ✅ |
| 2 | 100 | 216 (200) | 20 | 59.8 ms | 862 | 0.17 s | 0.33 s | 0 | 0 | ✅ |
| 3 | 100 | 217 (200) | 1 | 51.9 ms | 845 | 0.17 s | 0.25 s | 0 | 0 | ✅ |
| 4 | 100 | 216 (200) | 0 | 67.8 ms | 867 | 0.17 s | 0.32 s | 0 | 0 | ✅ |
| 5 | 100 | 217 (200) | 0 | 70.1 ms | 897 | 0.17 s | 0.25 s | 0 | 0 | ✅ |
| 6 | 100 | 216 (200) | 0 | 61.5 ms | 837 | 0.17 s | 0.40 s | 0 | 0 | ✅ |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 60–135 | 250 | 255 | 88.5 ms | 808.7 ms | 0.00 % | – | ✅ |
| 2 | 150–220 | 500 | 494 | 76.6 ms | 901.9 ms | 0.00 % | – | ✅ |
| 3 | 235–315 | 1000 | 953 | 77.5 ms | 846.2 ms | 0.00 % | – | ✅ |
| 4 | 330–525 | 2000 | 831 | 88.7 ms | 1,387.2 ms | 0.00 % | – | ❌ |
| 5 | 540–630 | 3000 | 1,418 | 70.3 ms | 754.8 ms | 0.00 % | – | ❌ |
| 6 | 522–582 | 4000 | 1,337 | 71.7 ms | 772.9 ms | 0.00 % | – | ❌ |

## CPU of the system under test (cores)

Load tools: producer, notifications service and load generator, where they run on the same machine. Machine busy and steal: all CPUs of the machine (node exporter).

| Stage | consumers | api | postgres | monitoring | Total | Load tools | Machine busy | Steal | Memory |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.28 | 0.03 | 0.12 | 0.04 | 0.48 | 0.07 | 35 % | 1.5 % | 1,097 MB |
| 2 | 0.31 | 0.07 | 0.14 | 0.05 | 0.58 | 0.08 | 40 % | 3.0 % | 1,103 MB |
| 3 | 0.29 | 0.11 | 0.13 | 0.04 | 0.58 | 0.07 | 42 % | 2.1 % | 1,106 MB |
| 4 | 0.30 | 0.11 | 0.09 | 0.05 | 0.56 | 0.08 | 43 % | 2.9 % | 1,107 MB |
| 5 | 0.31 | 0.13 | 0.14 | 0.05 | 0.64 | 0.08 | 45 % | 3.1 % | 1,109 MB |
| 6 | 0.31 | 0.14 | 0.14 | 0.05 | 0.64 | 0.09 | 46 % | 3.6 % | 1,115 MB |

## Result

Last stage that passed: 3 (100 experiments, 1000 req/s). Stage 4 (100 experiments, 2000 req/s) failed on the API.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

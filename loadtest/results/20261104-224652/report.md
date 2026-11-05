# Load test 20261104-224652

2 stages of 45 s, experiments of 15 s with 2 sensors at 1 sample/s, 108 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers (Kafka → Postgres → notifications)

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 10 | 24 (20) | 0 | 4.9 ms | 95 | 0.18 s | 0.83 s | 0 | 0 | ✅ |
| 2 | 20 | 0 (40) | 0 | 0.0 ms | 0 | – | – | 0 | 0 | ❌ |

## HTTP API (finished experiments only)

| Stage | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|
| 1 | 50 | 51 | 2.6 ms | 7.7 ms | 0.00 % | – | ✅ |
| 2 | 100 | 100 | 2.5 ms | 5.0 ms | 0.00 % | – | ✅ |

## CPU of the system under test (cores)

| Stage | consumers | api | postgres | monitoring | Total | Memory |
|---|---|---|---|---|---|---|
| 1 | 0.05 | 0.01 | 0.04 | 0.03 | 0.12 | 1,048 MB |
| 2 | 0.02 | 0.01 | 0.03 | 0.02 | 0.08 | 1,059 MB |

## Result

Last stage that passed: 1 (10 experiments, 50 req/s). Stage 2 (20 experiments, 100 req/s) failed on the consumers.

⚠️ Stage 2: the consumers got 0 of 40 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

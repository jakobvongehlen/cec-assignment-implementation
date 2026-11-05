# Load test 20261104-225029

6 stages of 90 s, experiments of 30 s with 2 sensors at 1 sample/s, 9520 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers (Kafka → Postgres → notifications)

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 50 | 115 (100) | 4 | 6.7 ms | 694 | 0.17 s | 1.20 s | 0 | 0 | ✅ |
| 2 | 100 | 187 (200) | 14 | 5.9 ms | 706 | 0.19 s | 1.50 s | 0 | 0 | ✅ |
| 3 | 200 | 156 (400) | 57 | 10.1 ms | 592 | 0.30 s | 2.89 s | 0 | 1 | ❌ |
| 4 | 400 | 594 (800) | 110 | 10.5 ms | 2,404 | 2.26 s | 5.68 s | 0 | 11 | ❌ |
| 5 | 800 | 575 (1600) | 160 | 8.4 ms | 2,216 | 2.32 s | 6.87 s | 0 | 132 | ❌ |
| 6 | 1600 | 509 (3200) | 147 | 9.2 ms | 1,954 | 1.67 s | 6.71 s | 0 | 219 | ❌ |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 55–130 | 250 | 258 | 2.7 ms | 16.9 ms | 0.00 % | – | ✅ |
| 2 | 145–215 | 500 | 518 | 2.9 ms | 20.7 ms | 0.00 % | – | ✅ |
| 3 | 230–305 | 1000 | 1,012 | 2.9 ms | 23.0 ms | 0.00 % | – | ✅ |
| 4 | 320–390 | 2000 | 2,062 | 4.7 ms | 47.2 ms | 10.65 % | ValidationError: 15,382 | ❌ |
| 5 | 405–480 | 4000 | 4,131 | 3.8 ms | 46.6 ms | 18.61 % | ValidationError: 57,644 | ❌ |
| 6 | 495–565 | 8000 | 8,265 | 4.1 ms | 44.2 ms | 19.08 % | ValidationError: 110,364 | ❌ |

## CPU of the system under test (cores)

| Stage | consumers | api | postgres | monitoring | Total | Memory |
|---|---|---|---|---|---|---|
| 1 | 0.11 | 0.01 | 0.06 | 0.03 | 0.21 | 1,083 MB |
| 2 | 0.14 | 0.02 | 0.07 | 0.03 | 0.26 | 1,081 MB |
| 3 | 0.13 | 0.05 | 0.04 | 0.03 | 0.25 | 1,077 MB |
| 4 | 0.37 | 0.10 | 0.15 | 0.04 | 0.66 | 1,121 MB |
| 5 | 0.29 | 0.23 | 0.13 | 0.03 | 0.69 | 1,200 MB |
| 6 | 0.28 | 0.32 | 0.13 | 0.04 | 0.77 | 1,260 MB |

## Result

Last stage that passed: 2 (100 experiments, 500 req/s). Stage 3 (200 experiments, 1000 req/s) failed on the consumers.

⚠️ Stage 3: the consumers got 156 of 400 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

⚠️ Stage 4: the consumers got 594 of 800 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

⚠️ Stage 5: the consumers got 575 of 1600 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

⚠️ Stage 6: the consumers got 509 of 3200 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

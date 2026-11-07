# Load test 20261104-230331

3 stages of 90 s, experiments of 30 s with 2 sensors at 1 sample/s, 8960 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 400 | 426 (800) | 1,197 | 5.5 ms | 2,610 | 2.03 s | 13.64 s | 54 | 17 | fail |
| 2 | 800 | 703 (1600) | 1,277 | 4.8 ms | 2,711 | 3.38 s | 9.97 s | 27 | 111 | fail |
| 3 | 1600 | 608 (3200) | 30 | 4.7 ms | 2,316 | 2.56 s | 6.70 s | 0 | 196 | fail |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 42–132 | 500 | 394 | 2.9 ms | 19.6 ms | 0.00 % | – | fail |
| 2 | 162–222 | 500 | 518 | 3.5 ms | 36.0 ms | 0.00 % | – | pass |
| 3 | 252–312 | 500 | 518 | 3.1 ms | 22.8 ms | 0.00 % | – | pass |

## CPU of the system under test (cores)

| Stage | consumers | api | postgres | monitoring | Total | Memory |
|---|---|---|---|---|---|---|
| 1 | 0.19 | 0.02 | 0.07 | 0.03 | 0.31 | 1,281 MB |
| 2 | 0.33 | 0.03 | 0.16 | 0.03 | 0.55 | 1,337 MB |
| 3 | 0.32 | 0.03 | 0.13 | 0.03 | 0.50 | 1,340 MB |

## Result

No stage passed. Stage 1 (400 experiments, 500 req/s) failed on the consumers and API.

Warning: stage 2: the consumers got 703 of 1600 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

Warning: stage 3: the consumers got 608 of 3200 events/s without falling behind, so the producer did not deliver the load (Kafka or network trouble, see tools.log). The stage says nothing about the consumers.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

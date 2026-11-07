# Load test vm-20261105-002957

3 stages of 120 s, experiments of 30 s with 2 sensors at 1 sample/s, 6630 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 450 | 973 (900) | 328 | 76.7 ms | 7,608 | 0.21 s | 0.91 s | 0 | 0 | pass |
| 2 | 500 | 1,053 (1000) | 4,945 | 89.7 ms | 6,102 | 2.83 s | 14.23 s | 314 | 0 | fail |
| 3 | 550 | 990 (1100) | 26,558 | 92.5 ms | 5,619 | 15.89 s | 53.11 s | 4,180 | 0 | fail |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 60–165 | 100 | 102 | 86.9 ms | 511.8 ms | 0.00 % | – | pass |
| 2 | 180–285 | 150 | 151 | 152.0 ms | 953.8 ms | 0.00 % | – | pass |
| 3 | 300–400 | 200 | 202 | 234.8 ms | 917.1 ms | 0.00 % | – | pass |

## CPU of the system under test (cores)

Load tools: producer, notifications service and load generator, where they run on the same machine. Machine busy and steal: all CPUs of the machine (node exporter).

| Stage | consumers | api | postgres | monitoring | Total | Load tools | Machine busy | Steal | Memory |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.03 | 0.02 | 0.31 | 0.05 | 1.41 | 0.27 | 94 % | 0.6 % | 684 MB |
| 2 | 1.08 | 0.03 | 0.32 | 0.05 | 1.48 | 0.31 | 100 % | 0.0 % | 768 MB |
| 3 | 1.05 | 0.04 | 0.32 | 0.05 | 1.47 | 0.35 | 100 % | 0.0 % | 822 MB |

## Result

Last stage that passed: 1 (450 experiments, 100 req/s). Stage 2 (500 experiments, 150 req/s) failed on the consumers.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

# Load test vm-20261105-000034

6 stages of 90 s, experiments of 30 s with 2 sensors at 1 sample/s, 10040 experiments in total. Each stage is measured over its steady window, after the previous stage's experiments have ended.

## Consumers

| Stage | Experiments | Events/s (expected) | Max lag | Processing p99 | Notifications | Latency p50 | Latency p99 | > 10 s | Errors | OK |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 216 (200) | 2 | 76.1 ms | 1,266 | 0.17 s | 0.38 s | 0 | 0 | pass |
| 2 | 200 | 432 (400) | 22 | 50.0 ms | 1,767 | 0.19 s | 0.49 s | 0 | 0 | pass |
| 3 | 400 | 870 (800) | 146 | 74.3 ms | 3,357 | 0.20 s | 0.82 s | 0 | 0 | pass |
| 4 | 600 | 1,001 (1200) | 17,729 | 93.2 ms | 3,962 | 7.52 s | 28.95 s | 1,501 | 0 | fail |
| 5 | 800 | 933 (1600) | 64,032 | 94.5 ms | 3,699 | 38.70 s | 59.57 s | 3,699 | 0 | fail |
| 6 | 1200 | 993 (2400) | 97,584 | 91.4 ms | 3,830 | 60.00 s | 60.00 s | 3,830 | 0 | fail |

## HTTP API (finished experiments only)

| Stage | Measured (s after start) | Target req/s | Achieved req/s | RTT p50 | RTT p99 | Error share | Errors | OK |
|---|---|---|---|---|---|---|---|---|
| 1 | 60–135 | 500 | 468 | 306.5 ms | 1,515.8 ms | 0.00 % | – | fail |
| 2 | 150–635 | 1000 | 4 | 847.2 ms | 10,000.0 ms | 0.00 % | – | fail |
| 3 | 252–312 | 2000 | 19 | 882.1 ms | 10,000.0 ms | 0.00 % | – | fail |
| 4 | 342–402 | 3000 | 0 | 10,000.0 ms | 10,000.0 ms | 0.00 % | – | fail |
| 5 | 432–492 | 4000 | 19 | 804.0 ms | 10,000.0 ms | 0.00 % | – | fail |
| 6 | 522–582 | 6000 | 0 | 0.0 ms | 0.0 ms | – | – | fail |

## CPU of the system under test (cores)

Load tools: producer, notifications service and load generator, where they run on the same machine. Machine busy and steal: all CPUs of the machine (node exporter).

| Stage | consumers | api | postgres | monitoring | Total | Load tools | Machine busy | Steal | Memory |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 0.30 | 0.06 | 0.13 | 0.05 | 0.54 | 0.08 | 39 % | 4.0 % | 915 MB |
| 2 | 0.52 | 0.01 | 0.18 | 0.04 | 0.75 | 0.11 | 51 % | 2.5 % | 939 MB |
| 3 | 0.94 | 0.01 | 0.29 | 0.05 | 1.29 | 0.25 | 87 % | 1.2 % | 947 MB |
| 4 | 1.04 | 0.02 | 0.32 | 0.05 | 1.44 | 0.35 | 100 % | 0.3 % | 1,013 MB |
| 5 | 1.03 | 0.02 | 0.25 | 0.05 | 1.35 | 0.39 | 100 % | 0.2 % | 1,105 MB |
| 6 | 1.05 | 0.01 | 0.29 | 0.05 | 1.40 | 0.37 | 100 % | 0.0 % | 1,132 MB |

## Result

No stage passed. Stage 1 (100 experiments, 500 req/s) failed on the API.

Pass criteria: no notification later than 10 s and none failed; the API answers ≥ 95% of the target rate with ≤ 1% errors or wrong answers.

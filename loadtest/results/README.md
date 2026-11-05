# Load test results

The runs behind the numbers in [`../README.md`](../README.md), kept with
[`../keep.sh`](../keep.sh). Each directory has `report.md` (the report),
`results.json` (its numbers), `plan.json` (the stages; `../plan.py` makes the
producer config from it), `t0` (start of the producer, epoch seconds) and the
logs, compressed: `xz -dc tools.log.xz | less`. Log lines are cut at 400
characters.

| Run | Where | Stages (experiments:req/s) | What it shows |
|---|---|---|---|
| [`20261104-224652`](20261104-224652/report.md) | dev machine | 10:50, 20:100 | Smoke test. In stage 2 the dev machine lost Kafka (DNS failures) and the producer stopped. |
| [`20261104-225029`](20261104-225029/report.md) | dev machine | 50:250 … 1600:8000 | The producer can't send more than ~650 events/s from the dev machine and loses messages (47k timeouts in `tools.log.xz`), which shows up as wrong API answers. The API kept up with 8,000 req/s. |
| [`20261104-230331`](20261104-230331/report.md) | dev machine | 400:500, 800:500, 1600:500 | Same with a pool of 8 Kafka clients in the producer: no better, so the limit is the dev machine's connection. |
| [`20261104-231120`](20261104-231120/report.md) | dev machine | 20:300, 40:600 | Check of the load generator fixes (stage windows, sending queued batches). |
| [`vm-20261105-000034`](vm-20261105-000034/report.md) | VM | 100:500 … 1200:6000 | **Consumer limit between 400 and 600 experiments.** The API stopped answering early in stage 2: 1,000 client connections, 1,024 open files (fixed since). |
| [`vm-20261105-001500`](vm-20261105-001500/report.md) | VM | 100:250 … 100:4000 | **100 experiments with up to ~1,400 req/s**: notifications unaffected (p99 ≤ 0.4 s). More requests don't get through the dev machine's connection. |
| [`vm-20261105-002957`](vm-20261105-002957/report.md) | VM | 450:100, 500:150, 550:200 | **The limit: 450 experiments pass, 500 don't.** |
| [`vm-20261105-011005`](vm-20261105-011005/report.md) | VM | 100:500, 100:1000 | Attempt to reproduce the API hang over the internet with 1,000 connections. The dev machine's connection broke first; reproduced on the VM instead ([`vm-20261105-api.md`](vm-20261105-api.md)). |
| [`vm-20261105-api.md`](vm-20261105-api.md) | VM | – | The API without the network: ~10,400 req/s. The hang and its fix. |

In the VM runs, the producer and the notifications service ran on the VM too
(their CPU is in the report as "Load tools"); the HTTP load came from the dev
machine.

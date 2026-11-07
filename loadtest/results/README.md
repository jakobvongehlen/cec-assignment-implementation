# Load test results

Runs kept with [`../keep.sh`](../keep.sh). Each directory has `report.md`, `results.json`, `plan.json`, `t0` (producer start, epoch seconds) and the compressed logs (`xz -dc tools.log.xz | less`). In the VM runs the producer and the notifications service ran on the VM too; the HTTP load came from the dev machine.

| Run | Where | Stages | Shows |
|---|---|---|---|
| [`20261104-224652`](20261104-224652/report.md) | dev | 10:50, 20:100 | Smoke test, lost Kafka in stage 2 |
| [`20261104-225029`](20261104-225029/report.md) | dev | 50:250 … 1600:8000 | The producer can't send more than about 650 events/s from the dev machine; the API kept up with 8,000 req/s |
| [`20261104-230331`](20261104-230331/report.md) | dev | 400:500 … 1600:500 | 8 Kafka clients in the producer don't help |
| [`20261104-231120`](20261104-231120/report.md) | dev | 20:300, 40:600 | Check of the load generator changes |
| [`vm-20261105-000034`](vm-20261105-000034/report.md) | VM | 100:500 … 1200:6000 | Consumer limit between 400 and 600 experiments; the API hang at 1,000 connections, fixed since |
| [`vm-20261105-001500`](vm-20261105-001500/report.md) | VM | 100:250 … 100:4000 | 100 experiments with up to about 1,400 req/s, notifications unaffected |
| [`vm-20261105-002957`](vm-20261105-002957/report.md) | VM | 450:100, 500:150, 550:200 | The limit: 450 experiments pass, 500 don't |
| [`vm-20261105-011005`](vm-20261105-011005/report.md) | VM | 100:500, 100:1000 | Attempt to reproduce the hang over the internet |
| [`vm-20261105-api.md`](vm-20261105-api.md) | VM | | The API without the network, the hang and its fix |

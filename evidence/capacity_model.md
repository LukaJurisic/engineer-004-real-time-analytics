# Capacity Model

This is directional planning math, not an AWS bill or cloud throughput benchmark.

| Scenario | Avg eps | Spike eps | MiB/s | Required shards | Recommended shards | Flink KPUs | Monthly range |
|---|---:|---:|---:|---:|---:|---:|---:|
| base | 578.7 | 5787.0 | 5.65 | 6 | 9 | 20 | $21621-$38918 |
| lean_budget | 578.7 | 2893.5 | 2.83 | 3 | 4 | 20 | $17605-$31689 |
| growth_500M_per_day | 5787.0 | 28935.2 | 28.26 | 29 | 44 | 104 | $29036-$52264 |
| largest_tenant_hotspot | 578.7 | 5787.0 | 5.65 | 6 | 9 | 20 | $21621-$38918 |
| larger_event_payload | 578.7 | 5787.0 | 22.61 | 23 | 35 | 46 | $24003-$43206 |

Each numeric field in `capacity_model.json` carries `source_label`, `source`, and `note` metadata.

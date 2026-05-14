# Sources

Access date: 2026-05-14.

## Challenge Sources

- Challenge repository: https://github.com/ericosiu/beat-claude
- Repository README: https://github.com/ericosiu/beat-claude/blob/main/README.md
- Public scoring guide: https://github.com/ericosiu/beat-claude/blob/main/SCORING.md
- Engineer 004 brief: https://github.com/ericosiu/beat-claude/blob/main/challenges/engineer-004/brief.md
- Engineer 004 scoring rubric: https://github.com/ericosiu/beat-claude/blob/main/challenges/engineer-004/scoring_rubric.md

## Official / Primary Sources

| Topic | Source | Used for |
|---|---|---|
| Kinesis shard write/read capacity | https://docs.aws.amazon.com/streams/latest/dev/working-with-streams.html | Kinesis shard supports 1 MB/s and 1000 records/s write, 2 MB/s read. |
| Kinesis on-demand, provisioned, warm throughput, hot-key caveats | https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html | Kinesis-first posture, warm/pre-provisioned launch guidance, hot partition warnings. |
| Kinesis quotas and retention | https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html | Stream throughput, default on-demand capacity, shard quotas, retention maximum. |
| Managed Service for Apache Flink KPU resource model | https://docs.aws.amazon.com/managed-flink/latest/java/how-resources.html | KPU CPU, memory, disk, parallelism concepts. |
| Managed Service for Apache Flink metrics | https://docs.aws.amazon.com/managed-flink/latest/java/metrics-dimensions.html | Checkpoint, memory, CPU, watermark, backpressure, and Kinesis lag runbook metrics. |
| Firehose Snowflake buffering | https://docs.aws.amazon.com/firehose/latest/APIReference/API_SnowflakeBufferingHints.html | Snowflake export freshness caveat and buffering range. |
| Firehose dynamic partition buffering | https://docs.aws.amazon.com/firehose/latest/dev/buffering.html | Firehose buffering and freshness caveats. |
| BigQuery Storage Write API overview | https://cloud.google.com/bigquery/docs/write-api | Default stream at-least-once semantics, application-created streams with offsets. |
| BigQuery Storage Write API best practices | https://cloud.google.com/bigquery/docs/write-api-best-practices | Offset retry semantics and exporter caveats. |
| ClickHouse insert strategy | https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy | Batch sizes, async insert return mode, insert reliability. |
| ClickHouse async inserts | https://clickhouse.com/docs/optimize/asynchronous-inserts | Async insert durability and failure mode caveats. |
| ClickHouse incremental materialized views | https://clickhouse.com/docs/materialized-view/incremental-materialized-view | Dashboard rollup strategy. |
| ClickHouse mutation avoidance | https://clickhouse.com/docs/optimize/avoid-mutations | GDPR delete tradeoff and mutation risk. |
| ClickHouse partitioning | https://clickhouse.com/docs/engines/table-engines/mergetree-family/custom-partitioning-key | Avoid tenant/client partitioning; use bounded/coarse partitioning. |

## Local Skills Used As Guidance

- `flink`: MSF KPU sizing caveats, source parallelism, checkpointing, state TTL, watermarks, and metrics.
- `aws-messaging-and-streaming`: Kinesis vs MSK, streaming vs messaging, fallback semantics.
- `clickhouse-architecture-advisor`: product analytics workload framing and ingestion/preaggregation decisions.
- `clickhouse-best-practices`: insert batching, async inserts, partitioning, materialized views, mutation avoidance.
- `aws-observability`: runbook metrics, alarm shape, cardinality warnings.
- `aws-billing-and-cost-management`: deterministic cost-calculation discipline and pricing hygiene.
- `ingesting-into-data-lake`: validation, reconciliation, schema evolution, and raw-lake caveats.

## Pricing Posture

No unit price in this submission is represented as a live AWS, ClickHouse, Snowflake, or BigQuery quote. `assumptions.yaml` contains editable assumed unit costs. Final purchasing requires region-specific pricing verification and architecture review.

# Runbook

## Primary Metrics

| Area | Metrics |
|---|---|
| Ingestion | ingest ack latency, validation rejects, stream write failures, fallback write failures |
| Fallback | fallback depth, oldest fallback age, replay success/failure, replay lag by tenant |
| Kinesis | write throttles, hot partition signals, shard count, iterator age / consumer lag |
| Flink | checkpoint duration, checkpoint size, failed checkpoints, backpressure, `millisBehindLatest`, CPU, heap, watermark lag |
| ClickHouse | insert lag, async insert errors, part counts, dashboard query p95/p99, mutation backlog |
| Data quality | accepted vs lake vs ClickHouse count deltas, duplicate rate, late-record rate |
| Compliance | erasure backlog, tombstone emissions, warehouse propagation status |
| Tenant controls | per-tenant throttles, noisy-neighbor isolation events, top tenant share |
| Exports | Snowflake delivery lag, BigQuery offset errors, export queue depth |

## First Response

- Confirm whether ingestion is rejecting before ACK or losing after ACK.
- If stream writes fail, verify fallback writes and fallback age before touching downstream sinks.
- If Flink lag grows, inspect backpressure, checkpoint duration, and source shard skew.
- If ClickHouse inserts lag, check batch size, async insert errors, part counts, and mutation pressure.
- If exports lag, protect core ingestion and dashboard paths before widening warehouse freshness.

## Rollback Steps

- Freeze tenant rollout flags.
- Route affected tenant reads to old system.
- Keep accepting new events only if durable stream or fallback writes are healthy.
- Snapshot reconciliation counters.
- Open incident log with exact affected tenants, event-time window, and accepted-event counts.

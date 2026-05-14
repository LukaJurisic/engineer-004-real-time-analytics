# What Breaks It

| Failure mode | Detection | Response |
|---|---|---|
| SDK lacks stable event IDs. | High fingerprint dedupe collisions or disputed customer counts. | Ship non-breaking SDK telemetry with stable IDs, retry counters, and delivery diagnostics. |
| Browser, network, or ad-blocker loss happens before ingestion. | Browser telemetry gaps, customer-side sampling, synthetic tests. | Keep outside accepted-event guarantee; improve SDK delivery telemetry. |
| Customer traffic skew creates hot shards. | Kinesis throttles, per-tenant partition heat, Flink subtask skew. | Increase tenant buckets, isolate tenant stream, or pre-provision shards. |
| Kinesis regional quota or warm throughput is not ready before launch. | Service Quotas review and launch readiness checklist. | Request quota increase, use provisioned/pre-warmed capacity, delay launch cutover if needed. |
| Flink checkpoint duration grows with state. | `lastCheckpointDuration`, `lastCheckpointSize`, failed checkpoints, lag. | Tighten TTL, add KPUs, rebalance keys, reduce state, adjust checkpoint interval. |
| BigQuery export backpressure. | Export lag, offset retry errors, queue depth. | Use application-created streams with offsets where needed, throttle exporter, buffer in lake. |
| ClickHouse mutation or erasure delay grows. | Mutation queue age, query p95/p99, delete backlog. | Prefer masking/tombstone strategy, schedule mutations, isolate compliance jobs. |
| Bot traffic or malicious tenants. | Rate-limit alerts, invalid-event spikes, cost anomalies. | Tenant throttles, WAF/bot controls, degraded mode before stream write. |
| Late or out-of-order events exceed watermark assumptions. | Late-record counters, reconciliation drift. | Widen allowed lateness or route late facts to correction path. |
| Legal requires immediate hard delete from immutable raw stores. | Privacy/legal review. | Escalate for retention exception, crypto-shredding, or raw-lake redesign. |
| Budget drops below assumptions. | Forecast exceeds budget dashboard. | Cut freshness targets, reduce retention, delay exports, or choose lower-cost managed options. |
| Dedicated senior engineers are pulled away. | Roadmap burndown and incident load. | Narrow MVP to ingestion, hot personalization, raw lake, and one dashboard path. |

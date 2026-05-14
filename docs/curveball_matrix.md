# Curveball Matrix

| Curveball | First response | Architecture impact | Evidence/demo to run | Human decision |
|---|---|---|---|---|
| Budget cut to $20K/month | Re-run assumptions with lean settings. | Lower retention/freshness, fewer rollups, delay exports. | `python src/capacity_model.py` | Which SLOs to relax. |
| Volume grows to 500M/day | Check shard/KPU and hot-tenant sensitivity. | More shards/KPUs, stronger tenant isolation. | `evidence/capacity_model.json` growth scenario | Buy capacity or narrow product scope. |
| Must use MSK instead of Kinesis | Switch stream spine ADR. | More ops burden, Kafka ecosystem gains. | `docs/adr/001-kinesis-vs-msk.md` | Whether team can operate Kafka. |
| Cannot use managed ClickHouse | Pick self-hosted or alternate OLAP. | More infra ownership, slower MVP. | `docs/adr/002-clickhouse-dashboard-store.md` | Accept ops cost or reduce analytics feature set. |
| BigQuery export must be near-real-time | Use Storage Write API exporter with offsets. | More exporter state and quota management. | `docs/sources.md` BigQuery sources | Pay for freshness and own offset complexity. |
| GDPR deletion SLA becomes stricter | Separate hot deletion from raw-store legal policy. | Crypto-shredding/delete manifests may become MVP. | `evidence/scenarios/gdpr_erasure.md` | Legal retention stance. |
| Largest tenant causes hot partitions | Increase buckets or isolate stream. | Tenant-specific capacity and cost. | `evidence/scenarios/largest_tenant_hotspot.md` | Commercial treatment of noisy tenant. |
| Existing SDK lacks event_id and retry telemetry | Rely on fingerprint dedupe for MVP. | Higher duplicate/collision ambiguity. | `evidence/scenarios/duplicate_and_out_of_order.md` | Approve non-breaking SDK telemetry release. |

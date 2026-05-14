# Real-Time Analytics Pipeline

Operating artifact [Observed: local artifact]: https://github.com/LukaJurisic/engineer-004-real-time-analytics

I would not promise zero browser-event loss [Assumed boundary] without an SDK change. Browser network failures, blockers, tab closes, and client retries happen before the server can own the event. I would promise zero accepted-event loss [Assumed production target; Observed: local simulation]: once the ingestion service durably writes an event to Kinesis or fallback spillover, it is replayable, deduped, reconciled, and auditable.

The company is moving from about 50M events/day [Benchmarked: challenge repo], 15-30 minute latency [Benchmarked: challenge repo], and roughly 3 percent peak loss [Benchmarked: challenge repo] to real-time dashboards, recent-behavior personalization, segments, warehouse exports, and deletion workflows for 500+ customers [Benchmarked: challenge repo]. The constraint that matters most is not raw technology choice; it is a 2-senior-engineer full-time allocation [Benchmarked: challenge repo] with an MVP in 3 months [Benchmarked: challenge repo] and a full system in 6 months [Benchmarked: challenge repo] under a $50K/month ceiling [Benchmarked: challenge repo].

## Recommendation

Use the existing SDK-compatible ingestion endpoint, then a Kinesis-first AWS pipeline:

`Ingestion API -> Kinesis Data Streams -> Managed Service for Apache Flink -> Redis hot state + ClickHouse dashboards + Amazon Simple Storage Service raw lake -> Snowflake/BigQuery exports`

Kinesis is my MVP choice because it reduces broker operations for the team size and timeline [Estimated]. I would keep the event contract portable enough that MSK/Kafka remains an escape hatch if replay tooling, Kafka ecosystem needs, per-tenant stream isolation, or large-scale cost justify the extra operational burden [Estimated]. I would not be casual about Kinesis on-demand. For known launches, I would provision or warm capacity, check regional quotas, and rehearse fallback because AWS documents hot-key and rapid-ramp caveats [Benchmarked: source].

The architecture diagram is in `docs/architecture.mmd` [Observed: local artifact].

## Contract And Processing

The event contract is typed in `src/event_contract.py` [Observed: local artifact]: tenant, event type, event time, visitor ID, optional user ID, optional event ID, session, properties, receipt time, and a derived dedupe key.

Dedupe rule: if `event_id` exists, use `tenant_id + event_id` [Assumed contract]. If it is missing, use a best-effort fingerprint across tenant, visitor/session/user identifier, event type, event timestamp bucket, and stable properties [Assumed contract]. That fingerprint is an MVP compatibility move, not a perfect substitute. It can suppress legitimate near-identical events [Estimated].

Tenant partitioning is concrete:

`partition_key = tenant_id + ":" + hash(visitor_or_session_id) % tenant_bucket_count`

Default `tenant_bucket_count` is 16 [Assumed]. Large tenants start at 64 buckets [Assumed] and can move to isolated streams if their traffic shape justifies it [Estimated]. Reconciliation is by tenant and event time, not by pretending there is total order across shards [Estimated].

The segment demo implements "viewed pricing 3+ times in 7 days" [Assumed segment rule]. Identity stitching accepts anonymous behavior before login, merges it on identify/login, and records conflicts instead of overwriting mappings [Observed: local simulation].

## SLOs And Scale

| Path | Target | Label |
|---|---:|---|
| Ingest ACK after receipt | p95 150 ms | [Assumed] |
| Accepted event to personalization hot state | p95 2 sec | [Assumed] |
| Accepted event to dashboard visibility | p95 5 sec | [Assumed] |
| Warehouse export freshness | 15 min | [Assumed] |
| Erasure workflow SLA | 30 days unless policy is stricter | [Assumed] |

I would keep those paths separate. During fallback replay, dashboard and personalization freshness can degrade while accepted-event durability remains intact [Observed: local simulation].

The base capacity model in `evidence/capacity_model.json` calculates 578.7 average events/sec [Estimated], 5,787.0 spike events/sec at a 10x spike [Estimated], 5.65 MiB/sec ingest at 1,024-byte average payloads [Estimated], 6 required Kinesis shards from the max records/bytes rule [Estimated using Benchmarked: source], 9 recommended shards with 1.5x headroom [Estimated], and 20 recommended Flink KPUs [Estimated]. Directional monthly cost is $21,621-$38,918 [Estimated], not a quote.

Those numbers are deliberately editable in `assumptions.yaml` [Observed: local artifact]. A 500M/day growth scenario [Assumed sensitivity] and largest-tenant hotspot scenario [Assumed sensitivity] are already generated.

## Reliability Semantics

The local simulator models the lifecycle explicitly:

`received -> rejected_invalid -> stream_write_failed -> fallback_write_failed -> not_accepted -> fallback_durable -> accepted -> processed | duplicate_deduped | erased_or_tombstoned | pending_replay`

The invariant is:

`accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count`

`lost_after_accept == 0` [Observed: local simulation]

The evidence command generated 6 scenario reports [Observed: local simulation]. In the stream outage scenario, 100 browser-generated events are received [Observed: local simulation], 80 stream writes fail [Observed: local simulation], 76 go to fallback [Observed: local simulation], 4 fallback writes fail and are not accepted [Observed: local simulation], and lost-after-accept remains 0 [Observed: local simulation]. That is the boundary I would defend in review.

This is at-least-once ingestion with idempotent processing [Estimated]. I would not market multi-sink exactly-once delivery. BigQuery can use Storage Write API offsets for exactly-once writes within application-created streams [Benchmarked: source], but the whole pipeline still needs reconciliation across Redis, ClickHouse, S3, Snowflake, and BigQuery [Estimated].

## Storage, Compliance, And Operations

Redis is for hot personalization state [Estimated]. ClickHouse is for dashboard OLAP with append-friendly tables, batched or async inserts, and incremental rollups [Benchmarked: source; Estimated]. S3 is the raw replay and reconciliation lake [Estimated]. Firehose/Snowflake and BigQuery exporters are downstream freshness paths, not the durability root [Estimated].

GDPR/CCPA deletion is a workflow, not a magic tombstone [Estimated]. Redis hot state, segment membership, and identity links can be removed quickly [Observed: local simulation]. ClickHouse deletes or masking can lag because mutations rewrite parts and must be monitored [Benchmarked: source]. S3 raw immutable logs require a legal-approved policy: crypto-shredding, delete manifests, partition expiration, or retention exceptions [Estimated]. Warehouse exports require customer-facing propagation contracts [Estimated]. Legal interpretation stays human [Estimated].

Operationally, I would alarm on ingest ack latency, stream write failures, fallback depth and age, Kinesis throttles, hot partitions, Flink checkpoint duration/size, failed checkpoints, backpressure, `millisBehindLatest`, ClickHouse insert lag and query p95/p99, discrepancy rate, erasure backlog, tenant throttles, and export lag [Estimated; Benchmarked: source for AWS/Flink metrics].

## Migration

Migration starts in shadow mode [Estimated]. Keep the existing SDK contract, mirror accepted events, compare old and new outputs by tenant and event time, then enable per-tenant read flags [Estimated]. Rollback is a tenant routing change plus reconciliation of the affected event-time window [Estimated]. I would keep the old system available in read-only fallback until dashboard parity and accepted-event reconciliation are boring [Estimated].

The proof packet is the part I would walk through live:

- `python -m pytest -q --cache-clear` passed 19 tests [Observed: local run].
- `python src/generate_evidence.py --reviewer-demo` generated manifest, capacity model, and scenario evidence [Observed: local run].
- `docs/evidence_log.md` maps claims to proof and limitations [Observed: local artifact].

What breaks this: missing SDK event IDs, browser delivery gaps, hot tenants, unprepared Kinesis quotas, growing Flink checkpoints, BigQuery backpressure, ClickHouse erasure delay, bot traffic, late events, stricter legal deletion, budget cuts, or losing the senior-engineer allocation [Estimated]. What stays human: migration approvals, retention policy, identity conflict policy, legal interpretation, budget-vs-latency calls, and incident go/no-go decisions [Estimated].

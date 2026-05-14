# Architecture

See `docs/architecture.mmd` for the diagram.

The core boundary is the ingestion acknowledgement. Browser-generated events are outside the server-side zero accepted-event loss guarantee. An event is accepted only after the ingestion service durably writes it to Kinesis Data Streams or to durable fallback spillover. Invalid, throttled, and fallback-write-failed events are not acknowledged and are not counted as accepted loss.

## Flow

- Existing SDK calls a compatible ingestion endpoint.
- The endpoint authenticates the tenant, validates the event contract, applies per-tenant limits, derives the dedupe key, and chooses the Kinesis partition key.
- Normal path writes to Kinesis Data Streams, then acknowledges.
- If stream write fails, the service writes the original event to fallback spillover and acknowledges only after that write is durable.
- Managed Service for Apache Flink consumes Kinesis, applies bounded event-time windows, identity stitching, segment rules, dedupe, and sink retries.
- Redis serves hot personalization state.
- ClickHouse serves dashboard analytics from raw events and rollups.
- S3 keeps the raw replayable lake in a Parquet/Iceberg-style layout.
- Snowflake and BigQuery exporters read from the lake or controlled export topics.
- Reconciliation compares accepted counts, sink counts, and lake counts by tenant and event time.

## Event Lifecycle

```text
received
  -> rejected_invalid
  -> stream_write_failed
       -> fallback_write_failed -> not_accepted
       -> fallback_durable -> accepted
  -> stream_durable -> accepted
accepted
  -> processed
  -> duplicate_deduped
  -> erased_or_tombstoned
  -> pending_replay
```

The local invariant is enforced by tests and scenarios:

```text
accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count
lost_after_accept == 0
```

## Tenant Partitioning

`partition_key = tenant_id + ":" + hash(visitor_or_session_id) % tenant_bucket_count`

The default bucket count is configurable in `assumptions.yaml`. Larger tenants get more buckets or isolated streams if their traffic shape justifies the operational cost. Reconciliation is keyed by tenant and event time, not by relying on total order across shards.

## ClickHouse Model

ClickHouse is the primary dashboard OLAP store because product analytics queries are high-cardinality, filter-heavy, and aggregation-heavy. The intended model is:

- raw append table ordered by tenant, event date/time, event type, and subject hash;
- monthly or coarse time partitioning, not partitioning by tenant identifiers;
- incremental materialized views for repeated dashboard rollups;
- batched inserts or async inserts with `wait_for_async_insert=1`;
- no frequent hard mutations on the hot path.

Deletes and GDPR masking are handled as a workflow, not as an instant hard-delete promise. Redis and derived segment state can be removed quickly. ClickHouse may use masking rows, lightweight deletes, or controlled mutations with monitoring. S3 raw immutable data needs legal-approved retention, crypto-shredding, delete manifests, partition expiration, or retention exceptions.

## Flink / MSF Notes

The Flink job keeps bounded state with TTL for identity, dedupe, and segment windows. Source parallelism should track Kinesis shard count. Watermarks use bounded out-of-orderness and idleness for low-volume tenants. Operational risk is checkpoint growth: checkpoint duration, checkpoint size, backpressure, and Kinesis lag are scale signals.

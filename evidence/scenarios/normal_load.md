# Scenario: normal_load

## Setup assumptions
520 tenants, one accepted event each, light pricing-page traffic.

## What failed or degraded
nothing intentionally degraded

## Key counters

```json
{
  "accepted_count": 520,
  "browser_generated_count": 520,
  "duplicate_deduped_count": 0,
  "erased_or_tombstoned_count": 0,
  "erasure_processed_count": 0,
  "fallback_used_count": 0,
  "fallback_write_failed_count": 0,
  "identity_conflict_count": 0,
  "lost_after_accept_count": 0,
  "not_accepted_count": 0,
  "pending_replay_count": 0,
  "processed_unique_count": 520,
  "received_count": 520,
  "rejected_invalid_count": 0,
  "scenario": "normal_load",
  "segment_membership_count": 0,
  "stream_write_failed_count": 0,
  "tombstones_emitted_count": 0
}
```

## Invariant result
accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count: True
lost_after_accept: 0

## What this does not prove
This is a deterministic semantic simulation. It does not prove AWS throughput, regional quota readiness, browser delivery, or ClickHouse query latency.

## Operator action if this happened in production
watch ingest ack p95, Kinesis throttles, Flink lag, and reconciliation deltas

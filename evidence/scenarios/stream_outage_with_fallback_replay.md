# Scenario: stream_outage_with_fallback_replay

## Setup assumptions
Kinesis write path is unavailable for the first local batch; fallback is durable except injected write failures.

## What failed or degraded
events accepted through fallback wait for replay; fallback-write failures are not acknowledged

## Key counters

```json
{
  "accepted_count": 96,
  "browser_generated_count": 100,
  "duplicate_deduped_count": 0,
  "erased_or_tombstoned_count": 0,
  "erasure_processed_count": 0,
  "fallback_used_count": 76,
  "fallback_write_failed_count": 4,
  "identity_conflict_count": 0,
  "lost_after_accept_count": 0,
  "not_accepted_count": 4,
  "pending_replay_count": 0,
  "processed_unique_count": 96,
  "received_count": 100,
  "rejected_invalid_count": 0,
  "scenario": "stream_outage_with_fallback_replay",
  "segment_membership_count": 0,
  "stream_write_failed_count": 80,
  "tombstones_emitted_count": 0
}
```

## Invariant result
accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count: True
lost_after_accept: 0

## What this does not prove
This is a deterministic semantic simulation. It does not prove AWS throughput, regional quota readiness, browser delivery, or ClickHouse query latency.

## Operator action if this happened in production
page on stream write failures, watch fallback age/depth, replay after stream recovery, reconcile by tenant and event time

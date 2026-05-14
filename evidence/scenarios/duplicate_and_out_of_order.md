# Scenario: duplicate_and_out_of_order

## Setup assumptions
behavior arrives before and after login, one duplicate retry, then a conflicting login.

## What failed or degraded
conflicting identity is recorded and the first mapping remains canonical

## Key counters

```json
{
  "accepted_count": 6,
  "browser_generated_count": 6,
  "duplicate_deduped_count": 1,
  "erased_or_tombstoned_count": 0,
  "erasure_processed_count": 0,
  "fallback_used_count": 0,
  "fallback_write_failed_count": 0,
  "identity_conflict_count": 1,
  "lost_after_accept_count": 0,
  "not_accepted_count": 0,
  "pending_replay_count": 0,
  "processed_unique_count": 5,
  "received_count": 6,
  "rejected_invalid_count": 0,
  "scenario": "duplicate_and_out_of_order",
  "segment_membership_count": 1,
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
send deterministic conflict record to review queue and reconcile downstream facts by canonical subject

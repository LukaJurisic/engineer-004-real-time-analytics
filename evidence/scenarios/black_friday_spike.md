# Scenario: black_friday_spike

## Setup assumptions
local spike multiplies normal event count and injects browser retries.

## What failed or degraded
duplicates are deduped; no stream outage in this scenario

## Key counters

```json
{
  "accepted_count": 2100,
  "browser_generated_count": 2100,
  "duplicate_deduped_count": 20,
  "erased_or_tombstoned_count": 0,
  "erasure_processed_count": 0,
  "fallback_used_count": 0,
  "fallback_write_failed_count": 0,
  "identity_conflict_count": 0,
  "lost_after_accept_count": 0,
  "not_accepted_count": 0,
  "pending_replay_count": 0,
  "processed_unique_count": 2080,
  "received_count": 2100,
  "rejected_invalid_count": 0,
  "scenario": "black_friday_spike",
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
pre-warm/provision stream capacity before launch and raise Kinesis quotas if forecast requires it

# Scenario: gdpr_erasure

## Setup assumptions
identified user qualifies for pricing segment, then sends erasure request.

## What failed or degraded
post-erasure events for the same identifiers are rejected until new consent policy says otherwise

## Key counters

```json
{
  "accepted_count": 5,
  "browser_generated_count": 6,
  "duplicate_deduped_count": 0,
  "erased_or_tombstoned_count": 1,
  "erasure_processed_count": 1,
  "fallback_used_count": 0,
  "fallback_write_failed_count": 0,
  "identity_conflict_count": 0,
  "lost_after_accept_count": 0,
  "not_accepted_count": 0,
  "pending_replay_count": 0,
  "processed_unique_count": 4,
  "received_count": 6,
  "rejected_invalid_count": 1,
  "scenario": "gdpr_erasure",
  "segment_membership_count": 0,
  "stream_write_failed_count": 0,
  "tombstones_emitted_count": 1
}
```

## Invariant result
accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count: True
lost_after_accept: 0

## What this does not prove
This is a deterministic semantic simulation. It does not prove AWS throughput, regional quota readiness, browser delivery, or ClickHouse query latency.

## Operator action if this happened in production
verify hot-state deletion, emit warehouse/delete manifests, and route legal-retention exceptions for approval

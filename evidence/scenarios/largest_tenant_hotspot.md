# Scenario: largest_tenant_hotspot

## Setup assumptions
one tenant exceeds local per-tenant limit while neighboring tenants continue.

## What failed or degraded
hot tenant is throttled and isolated before stream write

## Key counters

```json
{
  "accepted_count": 64,
  "browser_generated_count": 113,
  "duplicate_deduped_count": 0,
  "erased_or_tombstoned_count": 0,
  "erasure_processed_count": 0,
  "fallback_used_count": 0,
  "fallback_write_failed_count": 0,
  "identity_conflict_count": 0,
  "lost_after_accept_count": 0,
  "not_accepted_count": 0,
  "pending_replay_count": 0,
  "processed_unique_count": 64,
  "received_count": 113,
  "rejected_invalid_count": 49,
  "scenario": "largest_tenant_hotspot",
  "segment_membership_count": 3,
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
increase tenant bucket count or provision tenant-isolated stream if sustained and commercially justified

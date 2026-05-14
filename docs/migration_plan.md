# Migration Plan

## Shadow Mode

- Keep the existing SDK endpoint contract.
- Mirror accepted events into the new ingestion path while the old system remains authoritative.
- Compare accepted counts, raw lake counts, ClickHouse counts, and dashboard aggregates by tenant and event time.
- Sample event payloads for schema drift, dedupe behavior, and identity stitching.

## Compatibility

- No breaking SDK change for MVP.
- Server-side fingerprint dedupe handles missing event IDs.
- A future non-breaking SDK version should add stable event IDs, retry counters, delivery telemetry, and browser-side diagnostics.

## Rollout

- Per-tenant routing flags choose old, shadow, dual-read, or new-read mode.
- Start with internal tenants and low-risk customers.
- Move high-volume tenants only after capacity, quota, and reconciliation checks pass.
- Keep old reads available during the rollback window.

## Rollback

- Flip tenant routing back to old read path.
- Stop new dashboard reads for affected tenants.
- Continue writing accepted events to the raw lake for audit.
- Reconcile the rollback period before re-enabling new reads.

## Go / No-Go

- Ingest ack p95 within target.
- No accepted-event invariant breach.
- Fallback age and depth below runbook threshold.
- Reconciliation discrepancy within approved tolerance.
- Erasure workflow backlog within policy.
- Senior engineer and customer-success owner sign off for the tenant cohort.

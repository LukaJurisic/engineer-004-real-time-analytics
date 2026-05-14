# ADR 003: Accepted-Event Durability

## Decision

Guarantee zero accepted-event loss as the production target, not zero browser-event loss.

## Definition

An event is accepted only after the ingestion service durably writes it to Kinesis Data Streams or durable fallback spillover. From that point, the event must be replayable, deduped, reconciled, and auditable.

## Lifecycle

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

## Dedupe

- If `event_id` exists, use `tenant_id + event_id`.
- If missing, use a best-effort fingerprint from tenant, visitor/session/user identifier, event type, timestamp bucket, and stable properties.
- Fingerprint dedupe can suppress legitimate near-identical events and cannot replace stable SDK event IDs.

## Exactly-Once Limits

The end-to-end system is at-least-once with idempotent processing. Individual sinks may support stronger write semantics, but the whole multi-sink pipeline should be reconciled rather than marketed as exactly-once.

## SDK Improvements

A future non-breaking SDK version should add stable event IDs, retry counters, delivery telemetry, and browser-side delivery diagnostics.

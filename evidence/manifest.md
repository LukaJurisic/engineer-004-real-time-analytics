# Evidence Manifest

Command: `python src/generate_evidence.py --reviewer-demo`
Timestamp UTC: `2026-05-14T03:55:07.664802+00:00`

## Claims proven
- accepted-event lifecycle invariant holds for all local scenarios
- fallback-durable accepted events are replayable without accepted-event loss
- duplicates are deduped before segment membership changes
- identity conflicts are recorded instead of silently overwritten
- GDPR hot-state erasure emits tombstone/compaction command and removes segment membership
- capacity math is deterministic and source-labeled

## Claims not proven
- AWS regional throughput, quota readiness, or Kinesis warm-throughput behavior
- browser/network/ad-blocker delivery
- ClickHouse production query latency
- legal sufficiency of deletion policy
- final cloud bill

## Generated files
- `evidence/capacity_model.json`
- `evidence/capacity_model.md`
- `evidence/scenarios/normal_load.json`
- `evidence/scenarios/normal_load.md`
- `evidence/scenarios/black_friday_spike.json`
- `evidence/scenarios/black_friday_spike.md`
- `evidence/scenarios/stream_outage_with_fallback_replay.json`
- `evidence/scenarios/stream_outage_with_fallback_replay.md`
- `evidence/scenarios/largest_tenant_hotspot.json`
- `evidence/scenarios/largest_tenant_hotspot.md`
- `evidence/scenarios/gdpr_erasure.json`
- `evidence/scenarios/gdpr_erasure.md`
- `evidence/scenarios/duplicate_and_out_of_order.json`
- `evidence/scenarios/duplicate_and_out_of_order.md`

## Next files to inspect
- `SUBMISSION.md`
- `evidence/manifest.json`
- `evidence/scenarios/stream_outage_with_fallback_replay.md`
- `docs/evidence_log.md`
- `docs/sources.md`

# Evidence Log

| Claim | Source label | Proof tier | Evidence file | Notes / limitations |
|---|---|---:|---|---|
| Accepted-event lifecycle invariant holds in every local scenario. | [Observed: local simulation] | 3 | `evidence/manifest.json`, `tests/test_pipeline_semantics.py` | Proves local semantics only, not cloud throughput. |
| Stream outage can acknowledge through fallback and replay later without accepted-event loss. | [Observed: local simulation] | 3 | `evidence/scenarios/stream_outage_with_fallback_replay.md` | Fallback-write failures are not acknowledged. |
| Browser retries do not double-count segment membership. | [Observed: local simulation] | 3 | `tests/test_pipeline_semantics.py`, `evidence/scenarios/duplicate_and_out_of_order.md` | Fingerprint dedupe remains weaker than stable SDK event IDs. |
| Identity stitching converges for identify/login before and after behavior. | [Observed: local simulation] | 3 | `tests/test_identity_stitching.py` | Human policy still needed for conflicts. |
| Conflicting identity mappings are recorded, not silently overwritten. | [Observed: local simulation] | 3 | `tests/test_identity_stitching.py` | Does not decide customer-facing merge policy. |
| GDPR erasure removes hot state, segment membership, and identity links, then emits tombstone/compaction command. | [Observed: local simulation] | 3 | `tests/test_gdpr_erasure.py`, `evidence/scenarios/gdpr_erasure.md` | Not legal proof of compliance. |
| Base 50M events/day averages about 578.7 events/sec. | [Estimated] | 3 | `evidence/capacity_model.json`, `tests/test_capacity_model.py` | Calculation from challenge input. |
| Kinesis shard requirement uses max(records/sec / 1000, MiB/sec / 1). | [Benchmarked: source] + [Estimated] | 3 | `evidence/capacity_model.json`, `docs/sources.md` | Formula uses AWS shard capacity source. |
| Kinesis-first MVP is a better fit than operating MSK first for this team and timeline. | [Estimated] | 2 | `docs/adr/001-kinesis-vs-msk.md` | Judgment call, not a benchmark. |
| ClickHouse is appropriate for dashboard OLAP with batching, rollups, and cautious deletion mechanics. | [Benchmarked: source] + [Estimated] | 2 | `docs/adr/002-clickhouse-dashboard-store.md`, `docs/sources.md` | Needs production query tests before launch. |
| The main weak point is browser delivery and SDK telemetry, not accepted-event replay. | [Estimated] | 2 | `docs/what_breaks_it.md`, `docs/adr/003-accepted-event-durability.md` | Requires future SDK improvement to reduce browser-side ambiguity. |

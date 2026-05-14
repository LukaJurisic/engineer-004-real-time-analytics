# Walkthrough Script

Opening sentence:

> I separated impossible browser delivery promises from the server-side guarantee we can own: once ingestion durably accepts an event to Kinesis or fallback, it is replayable, deduped, reconciled, and auditable.

What to run:

```powershell
python -m pytest
python src/generate_evidence.py --reviewer-demo
```

What the files prove:

- `evidence/manifest.json`: all scenario invariants hold.
- `evidence/scenarios/stream_outage_with_fallback_replay.md`: fallback-accepted events replay, and failed fallback writes are not acknowledged.
- `evidence/scenarios/gdpr_erasure.md`: hot state, segment membership, and identity links are removed and a tombstone command is emitted.
- `evidence/capacity_model.json`: source-labeled capacity math and sensitivity scenarios.

What is intentionally not proven:

- AWS throughput or regional quota readiness.
- Browser delivery.
- ClickHouse production query latency.
- Final cloud bill.
- Legal sufficiency of erasure mechanics.

Likely questions and crisp answers:

- Why not MSK first? Kinesis is enough for the MVP and reduces ops load for the available team; MSK remains the escape hatch.
- Where can data still be lost? Before ingestion durably accepts it, especially browser/network/ad-blocker paths and failed fallback writes.
- Why ClickHouse? High-cardinality dashboard analytics need fast columnar OLAP; Redis remains for hot personalization.
- What is the riskiest part? Identity and deletion policy, because correctness depends on human policy and customer contracts, not only code.

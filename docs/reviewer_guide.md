# Reviewer Guide

## What to inspect in 5 minutes

- `SUBMISSION.md`
- `evidence/manifest.json`
- `evidence/scenarios/stream_outage_with_fallback_replay.md`
- `docs/evidence_log.md`
- `evidence/capacity_model.json`

## What this packet emphasizes

This is not just a service list. The repo implements a deterministic lifecycle model and tests the reliability claim that matters: no accepted event is lost after durable stream or fallback accept. The packet focuses on the failure boundary, replay, dedupe, noisy-neighbor controls, erasure limitations, and source-labeled capacity math.

## Demo commands

```powershell
python -m pytest
python src/generate_evidence.py --reviewer-demo
python src/simulate_pipeline.py --scenario stream_outage_with_fallback_replay
```

## Known limits

- Local simulation is not an AWS throughput benchmark.
- Cost model is directional and assumption-driven.
- Browser delivery loss is outside the accepted-event guarantee.
- Legal sufficiency of erasure policies stays human.

If you have 5 minutes:
1. Read SUBMISSION.md.
2. Run python -m pytest.
3. Run python src/generate_evidence.py --reviewer-demo.
4. Open evidence/manifest.json and evidence/scenarios/stream_outage_with_fallback_replay.md.
5. Read docs/evidence_log.md for claim-to-proof mapping.

# Accepted-Event Durability Proof Packet

This repo is a small failure-semantics lab for a real-time analytics migration. It does not benchmark AWS locally. It proves the reliability boundary the architecture depends on, then backs the plan with source-labeled capacity math and executable scenarios.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Python 3.11 or newer is expected. The artifact runs without AWS credentials.

## Reviewer Commands

```powershell
python -m pytest
python src/generate_evidence.py --reviewer-demo
```

The evidence command regenerates only known files under `evidence/` and leaves unknown files alone.

## Evidence Files

- `evidence/manifest.json`: command, timestamp, Python version, generated files, proven and unproven claims.
- `evidence/capacity_model.json`: source-labeled deterministic capacity and directional cost model.
- `evidence/scenarios/*.md`: scenario summaries with counters, invariant result, limitations, and operator actions.
- `docs/evidence_log.md`: claim-to-proof ledger.
- `docs/sources.md`: official/public sources and access date.

## Reviewer Checklist

- The reliability boundary is accepted events, not browser-generated events.
- The simulator asserts `accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count`.
- Fallback write failures are not counted as accepted loss because they are not acknowledged.
- Cost and scale numbers are editable assumptions or calculations, not quotes.
- Compliance language separates hot-state deletion from legal deletion policy.

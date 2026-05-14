# Verification Note

This note records the final local verification performed before packaging the submission.

## Checklist

| Requirement | Evidence | Status |
|---|---|---|
| Build only under `AI_challenges/SingleBrain/submission`. | All created artifact files are under this directory. | Complete |
| Read challenge source files. | Used the public challenge repository README, scoring guide, Engineer 004 brief, and scoring rubric listed in `docs/sources.md`. | Complete |
| Use required installed skills. | Read/use guidance from Flink, AWS messaging, ClickHouse architecture/best-practices, AWS observability, AWS billing, and data-lake skills; sources recorded in `docs/sources.md`. | Complete |
| Runnable artifact without AWS credentials. | `src/event_contract.py`, `src/simulate_pipeline.py`, `src/capacity_model.py`, `src/generate_evidence.py`. | Complete |
| Deterministic simulator with accepted-event lifecycle. | `src/simulate_pipeline.py`, `src/event_contract.py`, `tests/test_pipeline_semantics.py`. | Complete |
| Required lifecycle invariant. | Enforced by `PipelineStats.assert_invariants()` and all scenario tests. | Complete |
| Required scenarios. | `normal_load`, `black_friday_spike`, `stream_outage_with_fallback_replay`, `largest_tenant_hotspot`, `gdpr_erasure`, `duplicate_and_out_of_order` in `simulate_pipeline.py` and `evidence/scenarios/`. | Complete |
| Capacity/cost model with sensitivity analysis. | `assumptions.yaml`, `src/capacity_model.py`, `evidence/capacity_model.json`, `evidence/capacity_model.md`. | Complete |
| Required tests. | Five required test files exist under `tests/`; latest run: `python -m pytest -q --cache-clear` exited cleanly with 19 passing tests. | Complete |
| One-command reviewer demo. | `python src/generate_evidence.py --reviewer-demo`; latest run generated manifest, capacity model, and all scenario files. | Complete |
| README 5-minute path. | `README.md` begins with the exact requested 5-minute instructions. | Complete |
| Main senior-engineer memo. | `SUBMISSION.md`, first-person, concise, and focused on the submitted architecture. | Complete |
| Architecture diagram and explanation. | `docs/architecture.mmd`, `docs/architecture.md`. | Complete |
| Source-labeled numbers and sources. | `SUBMISSION.md`, `docs/sources.md`, `evidence/capacity_model.json`. | Complete |
| AI usage disclosure. | `docs/ai_usage.md`. | Complete |
| What breaks it and what stays human. | `docs/what_breaks_it.md`, `docs/what_stays_human.md`. | Complete |
| Migration plan and rollback. | `docs/migration_plan.md`. | Complete |
| Operational runbook. | `docs/runbook.md`. | Complete |
| Curveball matrix and walkthrough prep. | `docs/curveball_matrix.md`, `docs/walkthrough_script.md`. | Complete |
| ADRs. | `docs/adr/001-kinesis-vs-msk.md`, `002-clickhouse-dashboard-store.md`, `003-accepted-event-durability.md`. | Complete |
| Evidence log / claim ledger. | `docs/evidence_log.md`. | Complete |
| Source verification against official/public docs. | `docs/sources.md`; official AWS, GCP, and ClickHouse URLs recorded with access date. | Complete |
| No unqualified zero data loss claim. | `rg` check found no `zero data loss`; SUBMISSION uses accepted-event qualifier. | Complete |
| No fake production metrics or cloud benchmark claims. | Manifest states claims not proven; docs call local simulation semantic only. | Complete |
| Every number in SUBMISSION.md source-labeled. | PowerShell audit found no numeric lines lacking `[Assumed]`, `[Estimated]`, `[Benchmarked]`, or `[Observed]`. | Complete |
| Generated evidence files exist. | Manifest lists capacity model and six scenario JSON/MD pairs; required file check had no missing output. | Complete |

## Commands Verified

```powershell
python -m pytest
```

Result: `19 passed in 0.12s`.

```powershell
python -m pytest -q --cache-clear
```

Result: exited cleanly with 19 passing tests.

```powershell
python src/generate_evidence.py --reviewer-demo
```

Result: generated `evidence/manifest.json`, `evidence/capacity_model.json`, `evidence/capacity_model.md`, and all required scenario JSON/MD files.

## Remaining Limitations

- No AWS smoke test was run.
- Local simulation proves semantics, not cloud throughput.
- Directional cost model is not a quote.
- Legal deletion sufficiency remains a human/legal decision.

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import sys
from typing import Any

from capacity_model import write_outputs as write_capacity_outputs
from simulate_pipeline import SCENARIOS, run_all_scenarios, write_scenario_outputs


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = ROOT / "evidence"
SCENARIO_DIR = EVIDENCE_DIR / "scenarios"


def clear_known_generated_files() -> None:
    known_files = [
        EVIDENCE_DIR / "capacity_model.json",
        EVIDENCE_DIR / "capacity_model.md",
        EVIDENCE_DIR / "manifest.json",
        EVIDENCE_DIR / "manifest.md",
    ]
    for scenario in SCENARIOS:
        known_files.append(SCENARIO_DIR / f"{scenario}.json")
        known_files.append(SCENARIO_DIR / f"{scenario}.md")

    for path in known_files:
        if path.exists():
            path.unlink()


def generate(reviewer_demo: bool = False) -> dict[str, Any]:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    SCENARIO_DIR.mkdir(parents=True, exist_ok=True)
    clear_known_generated_files()

    capacity_paths = write_capacity_outputs(EVIDENCE_DIR)
    scenario_results = run_all_scenarios()
    scenario_paths = write_scenario_outputs(scenario_results, SCENARIO_DIR)

    generated_files = [path.relative_to(ROOT).as_posix() for path in capacity_paths.values()]
    generated_files.extend(path.relative_to(ROOT).as_posix() for path in scenario_paths)

    manifest = {
        "command": "python src/generate_evidence.py --reviewer-demo" if reviewer_demo else "python src/generate_evidence.py",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "platform": platform.platform(),
        "scenario_names": list(SCENARIOS.keys()),
        "source_label_policy": {
            "[Observed: local simulation]": "measured by deterministic tests/scripts in this repo",
            "[Benchmarked: source]": "verified against named official/public source in docs/sources.md",
            "[Estimated]": "calculated from stated assumptions or source facts",
            "[Assumed]": "editable planning assumption, not a measured fact",
        },
        "claims_proven": [
            "accepted-event lifecycle invariant holds for all local scenarios",
            "fallback-durable accepted events are replayable without accepted-event loss",
            "duplicates are deduped before segment membership changes",
            "identity conflicts are recorded instead of silently overwritten",
            "GDPR hot-state erasure emits tombstone/compaction command and removes segment membership",
            "capacity math is deterministic and source-labeled",
        ],
        "claims_not_proven": [
            "AWS regional throughput, quota readiness, or Kinesis warm-throughput behavior",
            "browser/network/ad-blocker delivery",
            "ClickHouse production query latency",
            "legal sufficiency of deletion policy",
            "final cloud bill",
        ],
        "generated_files": generated_files,
        "next_files_to_inspect": [
            "SUBMISSION.md",
            "evidence/manifest.json",
            "evidence/scenarios/stream_outage_with_fallback_replay.md",
            "docs/evidence_log.md",
            "docs/sources.md",
        ],
        "scenario_invariants": {
            result.name: result.stats.to_dict()["invariant"] for result in scenario_results
        },
    }

    manifest_json = EVIDENCE_DIR / "manifest.json"
    manifest_md = EVIDENCE_DIR / "manifest.md"
    manifest_json.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest_md.write_text(_manifest_markdown(manifest), encoding="utf-8")
    return manifest


def _manifest_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        "# Evidence Manifest",
        "",
        f"Command: `{manifest['command']}`",
        f"Timestamp UTC: `{manifest['timestamp_utc']}`",
        "",
        "## Claims proven",
    ]
    lines.extend(f"- {claim}" for claim in manifest["claims_proven"])
    lines.extend(["", "## Claims not proven"])
    lines.extend(f"- {claim}" for claim in manifest["claims_not_proven"])
    lines.extend(["", "## Generated files"])
    lines.extend(f"- `{path}`" for path in manifest["generated_files"])
    lines.extend(["", "## Next files to inspect"])
    lines.extend(f"- `{path}`" for path in manifest["next_files_to_inspect"])
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reviewer-demo", action="store_true")
    args = parser.parse_args()
    manifest = generate(reviewer_demo=args.reviewer_demo)

    print("Reviewer evidence generated.")
    print("Claims proven:")
    for claim in manifest["claims_proven"]:
        print(f"- {claim}")
    print("Claims not proven:")
    for claim in manifest["claims_not_proven"]:
        print(f"- {claim}")
    print("Generated files:")
    for path in manifest["generated_files"]:
        print(f"- {path}")
    print("Next files to inspect:")
    for path in manifest["next_files_to_inspect"]:
        print(f"- {path}")


if __name__ == "__main__":
    main()

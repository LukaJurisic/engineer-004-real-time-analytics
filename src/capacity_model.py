from __future__ import annotations

import argparse
from copy import deepcopy
import json
from math import ceil
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ASSUMPTIONS_PATH = ROOT / "assumptions.yaml"
EVIDENCE_DIR = ROOT / "evidence"

SECONDS_PER_DAY = 86_400
BYTES_PER_MIB = 1024 * 1024
KINESIS_RECORDS_PER_SHARD_SECOND = 1_000
KINESIS_MIB_PER_SHARD_SECOND = 1.0


def load_assumptions(path: Path = ASSUMPTIONS_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _field(value: float | int | str, unit: str, label: str, source: str, note: str = "") -> dict[str, Any]:
    return {"value": value, "unit": unit, "source_label": label, "source": source, "note": note}


def _merge_scenario(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    scenario = deepcopy(base)
    costs = scenario.get("unit_costs", {})
    for key, value in override.items():
        if key in costs:
            costs[key] = value
        else:
            scenario[key] = value
    scenario["unit_costs"] = costs
    return scenario


def calculate_scenario(name: str, assumptions: dict[str, Any]) -> dict[str, Any]:
    labels = assumptions["source_label_policy"]
    source_fact = "AWS Kinesis shard capacity docs; challenge brief for 50M/day input"
    scenario = _merge_scenario(assumptions["base"], assumptions.get("sensitivity", {}).get(name, {}))

    events_per_day = int(scenario["events_per_day"])
    event_size_bytes = int(scenario["event_size_bytes"])
    spike_multiplier = float(scenario["spike_multiplier"])
    headroom = float(scenario["headroom_multiplier"])
    compression_ratio = float(scenario["compression_ratio"])
    retention_hours = int(scenario["stream_retention_hours"])
    largest_tenant_share = float(scenario["largest_tenant_share"])

    average_events_per_second = events_per_day / SECONDS_PER_DAY
    spike_events_per_second = average_events_per_second * spike_multiplier
    ingest_mib_per_second = spike_events_per_second * event_size_bytes / BYTES_PER_MIB
    shard_requirement_records = spike_events_per_second / KINESIS_RECORDS_PER_SHARD_SECOND
    shard_requirement_mib = ingest_mib_per_second / KINESIS_MIB_PER_SHARD_SECOND
    required_shards = ceil(max(shard_requirement_records, shard_requirement_mib))
    recommended_shards = ceil(required_shards * headroom)

    raw_gib_per_day = events_per_day * event_size_bytes / (1024**3)
    compressed_gib_per_day = raw_gib_per_day * compression_ratio
    stream_retention_gib = raw_gib_per_day * (retention_hours / 24)
    monthly_raw_tib = raw_gib_per_day * scenario["monthly_days"] / 1024
    monthly_compressed_tib = compressed_gib_per_day * scenario["monthly_days"] / 1024

    flink_throughput_kpus = ceil(
        ingest_mib_per_second
        * float(scenario["flink_processing_amplification"])
        / float(scenario["flink_mb_per_kpu"])
    )
    flink_state_kpus = ceil(float(scenario["flink_state_gb"]) / float(scenario["usable_state_gb_per_kpu"]))
    flink_source_kpus = recommended_shards
    flink_base_kpus = max(flink_throughput_kpus, flink_state_kpus, flink_source_kpus)
    flink_recommended_kpus = max(2, int(ceil(flink_base_kpus * 1.3 / 2) * 2))

    largest_tenant_eps = spike_events_per_second * largest_tenant_share
    largest_tenant_bucket_count = int(scenario["large_tenant_bucket_count"])
    largest_tenant_eps_per_bucket = largest_tenant_eps / largest_tenant_bucket_count

    costs = scenario["unit_costs"]
    kinesis_month = recommended_shards * float(costs["kinesis_shard_month"])
    put_payload_units_million = (events_per_day * scenario["monthly_days"]) / 1_000_000
    kinesis_put_month = put_payload_units_million * float(costs["kinesis_put_payload_unit_million"])
    flink_month = flink_recommended_kpus * float(costs["flink_kpu_month"])
    s3_month = monthly_compressed_tib * float(costs["s3_storage_tb_month"])
    core_cost = (
        kinesis_month
        + kinesis_put_month
        + flink_month
        + s3_month
        + float(costs["clickhouse_month"])
        + float(costs["redis_month"])
        + float(costs["observability_month"])
        + float(costs["ingestion_compute_month"])
        + float(costs["warehouse_export_month"])
    )
    low_cost = core_cost * 0.75
    high_cost = core_cost * 1.35

    return {
        "scenario": name,
        "average_events_per_second": _field(
            round(average_events_per_second, 1),
            "events/sec",
            labels["estimated"],
            f"{events_per_day:,} events/day divided by 86,400 seconds",
        ),
        "spike_events_per_second": _field(
            round(spike_events_per_second, 1), "events/sec", labels["estimated"], "average_events_per_second * configured spike_multiplier"
        ),
        "ingest_mib_per_second": _field(
            round(ingest_mib_per_second, 2), "MiB/sec", labels["estimated"], "spike_events_per_second * average event size"
        ),
        "required_kinesis_shards": _field(
            required_shards,
            "shards",
            labels["estimated"],
            source_fact,
            "ceil(max(records/sec / 1000, MiB/sec / 1))",
        ),
        "recommended_kinesis_shards": _field(
            recommended_shards, "shards", labels["estimated"], "required_kinesis_shards * headroom_multiplier"
        ),
        "raw_gib_per_day": _field(round(raw_gib_per_day, 2), "GiB/day", labels["estimated"], "events/day * average event size"),
        "compressed_gib_per_day": _field(
            round(compressed_gib_per_day, 2), "GiB/day", labels["estimated"], "raw_gib_per_day * assumed compression ratio"
        ),
        "stream_retention_gib": _field(
            round(stream_retention_gib, 2), "GiB", labels["estimated"], "raw_gib_per_day * retention hours / 24"
        ),
        "monthly_raw_tib": _field(round(monthly_raw_tib, 2), "TiB/month", labels["estimated"], "raw_gib_per_day * 30 / 1024"),
        "monthly_compressed_tib": _field(
            round(monthly_compressed_tib, 2), "TiB/month", labels["estimated"], "compressed_gib_per_day * 30 / 1024"
        ),
        "flink_throughput_kpus": _field(
            flink_throughput_kpus,
            "KPUs",
            labels["estimated"],
            "Managed Service for Apache Flink KPU sizing heuristic from local skill, not AWS quote",
        ),
        "flink_state_kpus": _field(
            flink_state_kpus,
            "KPUs",
            labels["estimated"],
            "state size / assumed usable state memory per KPU",
        ),
        "flink_source_parallelism_kpus": _field(
            flink_source_kpus,
            "KPUs/task slots",
            labels["estimated"],
            "source parallelism matched to Kinesis shard count",
        ),
        "flink_recommended_kpus": _field(
            flink_recommended_kpus, "KPUs", labels["estimated"], "max throughput/state/source estimate with 30 percent headroom"
        ),
        "largest_tenant_events_per_second": _field(
            round(largest_tenant_eps, 1), "events/sec", labels["estimated"], "spike_events_per_second * largest_tenant_share"
        ),
        "largest_tenant_events_per_bucket_second": _field(
            round(largest_tenant_eps_per_bucket, 1),
            "events/sec/bucket",
            labels["estimated"],
            "largest tenant rate divided across configured tenant buckets",
        ),
        "directional_monthly_cost_low": _field(
            round(low_cost, 0), "USD/month", labels["estimated"], "assumed editable unit costs in assumptions.yaml"
        ),
        "directional_monthly_cost_high": _field(
            round(high_cost, 0), "USD/month", labels["estimated"], "assumed editable unit costs in assumptions.yaml"
        ),
        "cost_posture": _field(
            "directional range, not a quote",
            "text",
            labels["assumed"],
            "final purchasing requires region-specific AWS, ClickHouse, and warehouse pricing verification",
        ),
    }


def build_capacity_model(assumptions: dict[str, Any] | None = None) -> dict[str, Any]:
    assumptions = assumptions or load_assumptions()
    scenario_names = ["base"] + list(assumptions.get("sensitivity", {}).keys())
    return {
        "source_label_policy": assumptions["source_label_policy"],
        "scenarios": {name: calculate_scenario(name, assumptions) for name in scenario_names},
    }


def write_outputs(output_dir: Path = EVIDENCE_DIR) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    model = build_capacity_model()
    json_path = output_dir / "capacity_model.json"
    md_path = output_dir / "capacity_model.md"
    json_path.write_text(json.dumps(model, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lines = [
        "# Capacity Model",
        "",
        "This is directional planning math, not an AWS bill or cloud throughput benchmark.",
        "",
        "| Scenario | Avg eps | Spike eps | MiB/s | Required shards | Recommended shards | Flink KPUs | Monthly range |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for name, scenario in model["scenarios"].items():
        lines.append(
            "| {name} | {avg} | {spike} | {mib} | {required} | {recommended} | {kpus} | ${low}-${high} |".format(
                name=name,
                avg=scenario["average_events_per_second"]["value"],
                spike=scenario["spike_events_per_second"]["value"],
                mib=scenario["ingest_mib_per_second"]["value"],
                required=scenario["required_kinesis_shards"]["value"],
                recommended=scenario["recommended_kinesis_shards"]["value"],
                kpus=scenario["flink_recommended_kpus"]["value"],
                low=int(scenario["directional_monthly_cost_low"]["value"]),
                high=int(scenario["directional_monthly_cost_high"]["value"]),
            )
        )
    lines.extend(
        [
            "",
            "Each numeric field in `capacity_model.json` carries `source_label`, `source`, and `note` metadata.",
        ]
    )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"capacity_model_json": json_path, "capacity_model_md": md_path}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default=str(EVIDENCE_DIR))
    args = parser.parse_args()
    paths = write_outputs(Path(args.output_dir))
    for name, path in paths.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()

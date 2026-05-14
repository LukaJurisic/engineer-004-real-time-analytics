from capacity_model import build_capacity_model


def test_50m_per_day_average_events_per_second_is_about_578_7():
    model = build_capacity_model()
    value = model["scenarios"]["base"]["average_events_per_second"]["value"]
    assert value == 578.7


def test_shard_count_uses_max_records_or_mib_per_second():
    model = build_capacity_model()
    base = model["scenarios"]["base"]
    spike_eps = base["spike_events_per_second"]["value"]
    mib = base["ingest_mib_per_second"]["value"]
    expected = max(spike_eps / 1000, mib / 1)
    assert base["required_kinesis_shards"]["value"] == 6
    assert base["required_kinesis_shards"]["value"] >= expected


def test_headroom_applied_after_base_shard_requirement():
    model = build_capacity_model()
    base = model["scenarios"]["base"]
    assert base["required_kinesis_shards"]["value"] == 6
    assert base["recommended_kinesis_shards"]["value"] == 9


def test_sensitivity_scenarios_exist():
    model = build_capacity_model()
    assert {
        "base",
        "lean_budget",
        "growth_500M_per_day",
        "largest_tenant_hotspot",
        "larger_event_payload",
    }.issubset(model["scenarios"])


def test_source_labels_exist_on_numeric_outputs():
    model = build_capacity_model()
    for scenario in model["scenarios"].values():
        for field in scenario.values():
            if isinstance(field, dict) and isinstance(field.get("value"), (int, float)):
                assert field["source_label"]
                assert field["source"]

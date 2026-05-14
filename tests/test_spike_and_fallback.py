from simulate_pipeline import run_scenario


def test_stream_outage_uses_fallback_and_later_replay():
    result = run_scenario("stream_outage_with_fallback_replay")
    stats = result.stats
    assert stats.stream_write_failed_count > 0
    assert stats.fallback_used_count > 0
    assert stats.fallback_write_failed_count > 0
    assert stats.pending_replay_count == 0
    assert stats.lost_after_accept_count == 0
    stats.assert_invariants()


def test_largest_tenant_hotspot_triggers_throttling_without_stopping_others():
    result = run_scenario("largest_tenant_hotspot")
    stats = result.stats
    assert stats.tenant_throttles["tenant_0001"] > 0
    assert stats.processed_unique_count > 40
    assert any(key.startswith("tenant_0002") for key in result.summary["identity_links"]) is False
    assert stats.lost_after_accept_count == 0
    stats.assert_invariants()


def test_scenario_summaries_preserve_lost_after_accept_zero():
    for scenario in ["black_friday_spike", "stream_outage_with_fallback_replay", "largest_tenant_hotspot"]:
        result = run_scenario(scenario)
        assert result.stats.to_dict()["lost_after_accept_count"] == 0
        assert result.stats.to_dict()["invariant"]["holds"] is True

from event_contract import Event, EventType
from simulate_pipeline import PipelineSimulator, run_all_scenarios


def test_no_accepted_events_are_lost_after_durable_accept():
    for result in run_all_scenarios():
        stats = result.stats
        assert stats.lost_after_accept_count == 0
        assert stats.invariant_holds


def test_duplicate_browser_retries_do_not_double_count_segment_membership():
    sim = PipelineSimulator("duplicate_retry")
    tenant = "tenant_dupe"
    visitor = "visitor_dupe"
    for idx in range(3):
        event = Event(
            tenant_id=tenant,
            event_type=EventType.PAGE_VIEW,
            timestamp=1_700_000_000 + idx,
            visitor_id=visitor,
            event_id=f"pricing-{idx}",
            session_id="s1",
            properties={"url": "/pricing"},
        )
        sim.ingest_event(event)
        sim.ingest_event(event)

    assert sim.stats.duplicate_deduped_count == 3
    assert sim.stats.segment_membership_count == 1
    assert len(sim.hot_state[f"{tenant}:visitor:{visitor}"]) == 3
    sim.stats.assert_invariants()


def test_missing_event_id_uses_fingerprint_dedupe():
    sim = PipelineSimulator("fingerprint_dedupe")
    first = Event(
        tenant_id="tenant_fingerprint",
        event_type=EventType.CLICK,
        timestamp=1_700_000_000,
        visitor_id="visitor_fingerprint",
        event_id=None,
        session_id="session_fingerprint",
        properties={"target": "hero_cta", "url": "/pricing"},
    )
    retry = Event(
        tenant_id="tenant_fingerprint",
        event_type=EventType.CLICK,
        timestamp=1_700_000_003,
        visitor_id="visitor_fingerprint",
        event_id=None,
        session_id="session_fingerprint",
        properties={"target": "hero_cta", "url": "/pricing"},
    )

    assert first.dedupe_key == retry.dedupe_key
    sim.ingest_event(first)
    sim.ingest_event(retry)

    assert sim.stats.accepted_count == 2
    assert sim.stats.processed_unique_count == 1
    assert sim.stats.duplicate_deduped_count == 1
    sim.stats.assert_invariants()


def test_rejected_and_fallback_write_failed_events_are_not_accepted_loss():
    sim = PipelineSimulator("fallback_failure")
    failed = Event(
        tenant_id="tenant_fail",
        event_type=EventType.PAGE_VIEW,
        timestamp=1_700_000_000,
        visitor_id="visitor_fail",
        event_id="fail",
        session_id="s1",
        properties={"url": "/pricing"},
    )
    accepted = Event(
        tenant_id="tenant_ok",
        event_type=EventType.PAGE_VIEW,
        timestamp=1_700_000_001,
        visitor_id="visitor_ok",
        event_id="ok",
        session_id="s1",
        properties={"url": "/pricing"},
    )
    assert not sim.ingest_event(failed, stream_available=False, fallback_available=False)
    assert sim.ingest_event(accepted, stream_available=False, fallback_available=True)
    sim.replay_fallback()

    assert sim.stats.fallback_write_failed_count == 1
    assert sim.stats.not_accepted_count == 1
    assert sim.stats.accepted_count == 1
    assert sim.stats.lost_after_accept_count == 0
    sim.stats.assert_invariants()


def test_lifecycle_invariant_holds_for_every_scenario():
    for result in run_all_scenarios():
        result.stats.assert_invariants()

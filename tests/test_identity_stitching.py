from event_contract import Event, EventType
from simulate_pipeline import PipelineSimulator


def _pricing_event(tenant: str, visitor: str, idx: int, event_id: str | None = None) -> Event:
    return Event(
        tenant_id=tenant,
        event_type=EventType.PAGE_VIEW,
        timestamp=1_700_000_000 + idx,
        visitor_id=visitor,
        event_id=event_id or f"pricing-{idx}",
        session_id="s1",
        properties={"url": "/pricing"},
    )


def _login(tenant: str, visitor: str, user: str, idx: int) -> Event:
    return Event(
        tenant_id=tenant,
        event_type=EventType.LOGIN,
        timestamp=1_700_000_000 + idx,
        visitor_id=visitor,
        user_id=user,
        event_id=f"login-{idx}-{user}",
        session_id="s1",
        properties={"url": "/login"},
    )


def test_identify_before_and_after_behavior_converges_to_canonical_subject():
    tenant = "tenant_identity"

    after_behavior = PipelineSimulator("after_behavior")
    after_behavior.ingest_event(_pricing_event(tenant, "visitor_a", 1))
    after_behavior.ingest_event(_login(tenant, "visitor_a", "user_a", 2))
    after_behavior.ingest_event(_pricing_event(tenant, "visitor_a", 3))

    before_behavior = PipelineSimulator("before_behavior")
    before_behavior.ingest_event(_login(tenant, "visitor_b", "user_b", 1))
    before_behavior.ingest_event(_pricing_event(tenant, "visitor_b", 2))
    before_behavior.ingest_event(_pricing_event(tenant, "visitor_b", 3))

    assert f"{tenant}:user:user_a" in after_behavior.hot_state
    assert f"{tenant}:visitor:visitor_a" not in after_behavior.hot_state
    assert f"{tenant}:user:user_b" in before_behavior.hot_state
    after_behavior.stats.assert_invariants()
    before_behavior.stats.assert_invariants()


def test_conflicting_user_id_mappings_create_conflict_records():
    sim = PipelineSimulator("conflict")
    tenant = "tenant_identity"
    visitor = "visitor_conflict"
    sim.ingest_event(_login(tenant, visitor, "user_a", 1))
    sim.ingest_event(_login(tenant, visitor, "user_b", 2))

    assert sim.identity_links[(tenant, visitor)] == "user_a"
    assert sim.stats.identity_conflict_count == 1
    assert sim.identity_conflicts[0]["policy"] == "keep_first_mapping_and_record_conflict"
    sim.stats.assert_invariants()


def test_out_of_order_events_do_not_corrupt_hot_state():
    sim = PipelineSimulator("out_of_order")
    tenant = "tenant_identity"
    visitor = "visitor_out_of_order"
    sim.ingest_event(_pricing_event(tenant, visitor, 30, "later-1"))
    sim.ingest_event(_login(tenant, visitor, "user_c", 10))
    sim.ingest_event(_pricing_event(tenant, visitor, 11, "earlier-2"))

    key = f"{tenant}:user:user_c"
    assert key in sim.hot_state
    assert len(sim.hot_state[key]) == 2
    assert all("visitor:" not in subject for subject in sim.hot_state)
    sim.stats.assert_invariants()

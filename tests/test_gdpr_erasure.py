from event_contract import Event, EventType
from simulate_pipeline import PipelineSimulator


def _event(tenant: str, visitor: str, user: str | None, idx: int, event_type: EventType, url: str) -> Event:
    return Event(
        tenant_id=tenant,
        event_type=event_type,
        timestamp=1_700_000_000 + idx,
        visitor_id=visitor,
        user_id=user,
        event_id=f"{event_type.value}-{idx}",
        session_id="s1",
        properties={"url": url},
    )


def test_erasure_removes_hot_state_segment_membership_and_identity_links():
    sim = PipelineSimulator("erasure")
    tenant = "tenant_gdpr"
    visitor = "visitor_gdpr"
    user = "user_gdpr"
    sim.ingest_event(_event(tenant, visitor, None, 1, EventType.PAGE_VIEW, "/pricing"))
    sim.ingest_event(_event(tenant, visitor, None, 2, EventType.PAGE_VIEW, "/pricing"))
    sim.ingest_event(_event(tenant, visitor, user, 3, EventType.LOGIN, "/login"))
    sim.ingest_event(_event(tenant, visitor, user, 4, EventType.PAGE_VIEW, "/pricing"))
    assert sim.stats.segment_membership_count == 1

    sim.ingest_event(_event(tenant, visitor, user, 5, EventType.ERASURE_REQUEST, "/privacy"))

    assert sim.hot_state == {}
    assert sim.segment_membership == set()
    assert sim.identity_links == {}
    assert sim.stats.erasure_processed_count == 1
    sim.stats.assert_invariants()


def test_erasure_emits_tombstone_compaction_command():
    sim = PipelineSimulator("erasure_tombstone")
    tenant = "tenant_gdpr"
    visitor = "visitor_gdpr"
    user = "user_gdpr"
    sim.ingest_event(_event(tenant, visitor, user, 1, EventType.LOGIN, "/login"))
    sim.ingest_event(_event(tenant, visitor, user, 2, EventType.ERASURE_REQUEST, "/privacy"))

    assert sim.stats.tombstones_emitted_count == 1
    assert sim.tombstones[0]["compaction_command"].startswith(f"erase tenant={tenant}")
    sim.stats.assert_invariants()


def test_post_erasure_event_policy_is_explicit_and_tested():
    sim = PipelineSimulator("post_erasure")
    tenant = "tenant_gdpr"
    visitor = "visitor_gdpr"
    user = "user_gdpr"
    sim.ingest_event(_event(tenant, visitor, user, 1, EventType.ERASURE_REQUEST, "/privacy"))
    accepted = sim.ingest_event(_event(tenant, visitor, user, 2, EventType.PAGE_VIEW, "/pricing"))

    assert sim.post_erasure_policy == "drop_until_new_consent"
    assert not accepted
    assert sim.stats.accepted_count == 1
    assert sim.stats.rejected_invalid_count == 1
    sim.stats.assert_invariants()

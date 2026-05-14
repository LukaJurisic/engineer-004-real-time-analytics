from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import random
from typing import Callable, Iterable, Any

from event_contract import Event, EventLifecycleState, EventType, PipelineStats


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_SCENARIO_DIR = ROOT / "evidence" / "scenarios"
SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60


@dataclass
class ScenarioResult:
    name: str
    stats: PipelineStats
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.name,
            "stats": self.stats.to_dict(),
            "summary": self.summary,
        }


class PipelineSimulator:
    post_erasure_policy = "drop_until_new_consent"

    def __init__(
        self,
        scenario_name: str,
        tenant_count: int = 520,
        tenant_bucket_count: int = 16,
        tenant_rate_limit: int = 10_000,
        seed: int = 42,
    ) -> None:
        self.scenario_name = scenario_name
        self.tenant_count = tenant_count
        self.tenant_bucket_count = tenant_bucket_count
        self.tenant_rate_limit = tenant_rate_limit
        self.rng = random.Random(seed)
        self.stats = PipelineStats(scenario=scenario_name)
        self.processed_dedupe_keys: set[str] = set()
        self.fallback_queue: list[Event] = []
        self.stream_records: list[Event] = []
        self.per_tenant_received: dict[str, int] = {}
        self.identity_links: dict[tuple[str, str], str] = {}
        self.user_to_visitors: dict[tuple[str, str], set[str]] = {}
        self.identity_conflicts: list[dict[str, str]] = []
        self.hot_state: dict[str, dict[str, int]] = {}
        self.segment_membership: set[str] = set()
        self.erased_identifiers: set[tuple[str, str, str]] = set()
        self.tombstones: list[dict[str, Any]] = []

    def ingest_event(
        self,
        event: Event,
        *,
        stream_available: bool = True,
        fallback_available: bool = True,
        process_immediately: bool = True,
    ) -> bool:
        self.stats.browser_generated_count += 1
        self.stats.received_count += 1
        self.stats.record_lifecycle(EventLifecycleState.RECEIVED)

        if not self._is_valid(event):
            self._reject()
            return False

        self.per_tenant_received[event.tenant_id] = self.per_tenant_received.get(event.tenant_id, 0) + 1
        if self.per_tenant_received[event.tenant_id] > self.tenant_rate_limit:
            self.stats.tenant_throttles[event.tenant_id] = self.stats.tenant_throttles.get(event.tenant_id, 0) + 1
            self._reject()
            return False

        if event.event_type != EventType.ERASURE_REQUEST and self._matches_erased_identifier(event):
            self._reject()
            return False

        if stream_available:
            self.stream_records.append(event)
            self.stats.accepted_count += 1
            self.stats.record_lifecycle(EventLifecycleState.STREAM_DURABLE)
            self.stats.record_lifecycle(EventLifecycleState.ACCEPTED)
            if process_immediately:
                self.process_event(event)
            return True

        self.stats.stream_write_failed_count += 1
        self.stats.record_lifecycle(EventLifecycleState.STREAM_WRITE_FAILED)
        if fallback_available:
            self.fallback_queue.append(event)
            self.stats.accepted_count += 1
            self.stats.fallback_used_count += 1
            self.stats.pending_replay_count += 1
            self.stats.record_lifecycle(EventLifecycleState.FALLBACK_DURABLE)
            self.stats.record_lifecycle(EventLifecycleState.ACCEPTED)
            self.stats.record_lifecycle(EventLifecycleState.PENDING_REPLAY)
            return True

        self.stats.fallback_write_failed_count += 1
        self.stats.not_accepted_count += 1
        self.stats.record_lifecycle(EventLifecycleState.FALLBACK_WRITE_FAILED)
        self.stats.record_lifecycle(EventLifecycleState.NOT_ACCEPTED)
        return False

    def replay_fallback(self) -> None:
        replaying = list(self.fallback_queue)
        self.fallback_queue.clear()
        for event in replaying:
            if self.stats.pending_replay_count <= 0:
                self.stats.lost_after_accept_count += 1
                continue
            self.stats.pending_replay_count -= 1
            self.process_event(event)

    def process_event(self, event: Event) -> None:
        if event.dedupe_key in self.processed_dedupe_keys:
            self.stats.duplicate_deduped_count += 1
            self.stats.record_lifecycle(EventLifecycleState.DUPLICATE_DEDUPED)
            return

        self.processed_dedupe_keys.add(event.dedupe_key)
        if event.event_type == EventType.ERASURE_REQUEST:
            self._process_erasure(event)
            self.stats.erased_or_tombstoned_count += 1
            self.stats.record_lifecycle(EventLifecycleState.ERASED_OR_TOMBSTONED)
            return

        self._apply_identity_event(event)
        self._apply_behavior(event)
        self.stats.processed_unique_count += 1
        self.stats.record_lifecycle(EventLifecycleState.PROCESSED)

    def _reject(self) -> None:
        self.stats.rejected_invalid_count += 1
        self.stats.record_lifecycle(EventLifecycleState.REJECTED_INVALID)

    def _is_valid(self, event: Event) -> bool:
        if not event.tenant_id or event.timestamp <= 0:
            return False
        if event.event_type == EventType.ERASURE_REQUEST:
            return bool(event.user_id or event.visitor_id)
        return bool(event.visitor_id or event.user_id or event.session_id)

    def _matches_erased_identifier(self, event: Event) -> bool:
        return any(identifier in self.erased_identifiers for identifier in event.subject_identifiers())

    def _canonical_subject(self, event: Event) -> str:
        if event.visitor_id and (event.tenant_id, event.visitor_id) in self.identity_links:
            return f"user:{self.identity_links[(event.tenant_id, event.visitor_id)]}"
        if event.user_id:
            return f"user:{event.user_id}"
        if event.visitor_id:
            return f"visitor:{event.visitor_id}"
        return f"session:{event.session_id}"

    def _subject_key(self, tenant_id: str, subject: str) -> str:
        return f"{tenant_id}:{subject}"

    def _apply_identity_event(self, event: Event) -> None:
        if event.event_type not in {EventType.IDENTIFY, EventType.LOGIN}:
            return
        if not event.visitor_id or not event.user_id:
            return

        key = (event.tenant_id, event.visitor_id)
        existing = self.identity_links.get(key)
        if existing and existing != event.user_id:
            conflict = {
                "tenant_id": event.tenant_id,
                "visitor_id": event.visitor_id,
                "existing_user_id": existing,
                "new_user_id": event.user_id,
                "policy": "keep_first_mapping_and_record_conflict",
            }
            self.identity_conflicts.append(conflict)
            self.stats.identity_conflict_count = len(self.identity_conflicts)
            return

        self.identity_links[key] = event.user_id
        self.user_to_visitors.setdefault((event.tenant_id, event.user_id), set()).add(event.visitor_id)
        self._merge_visitor_state_into_user(event.tenant_id, event.visitor_id, event.user_id)

    def _merge_visitor_state_into_user(self, tenant_id: str, visitor_id: str, user_id: str) -> None:
        visitor_key = self._subject_key(tenant_id, f"visitor:{visitor_id}")
        user_key = self._subject_key(tenant_id, f"user:{user_id}")
        visitor_state = self.hot_state.pop(visitor_key, {})
        user_state = self.hot_state.setdefault(user_key, {})
        for dedupe_key, timestamp in visitor_state.items():
            user_state[dedupe_key] = timestamp
        self.segment_membership.discard(visitor_key)
        self._refresh_pricing_segment(user_key)

    def _apply_behavior(self, event: Event) -> None:
        if event.event_type != EventType.PAGE_VIEW:
            return
        if event.properties.get("url") != "/pricing":
            return

        subject = self._canonical_subject(event)
        subject_key = self._subject_key(event.tenant_id, subject)
        state = self.hot_state.setdefault(subject_key, {})
        state[event.dedupe_key] = event.timestamp
        cutoff = event.timestamp - SEVEN_DAYS_SECONDS
        stale = [key for key, timestamp in state.items() if timestamp < cutoff]
        for key in stale:
            del state[key]
        self._refresh_pricing_segment(subject_key)

    def _refresh_pricing_segment(self, subject_key: str) -> None:
        if len(self.hot_state.get(subject_key, {})) >= 3:
            self.segment_membership.add(subject_key)
        else:
            self.segment_membership.discard(subject_key)
        self.stats.segment_membership_count = len(self.segment_membership)

    def _process_erasure(self, event: Event) -> None:
        subject = self._canonical_subject(event)
        subject_key = self._subject_key(event.tenant_id, subject)
        removed_hot_keys = []

        if event.user_id:
            for visitor_id in self.user_to_visitors.get((event.tenant_id, event.user_id), set()).copy():
                visitor_subject_key = self._subject_key(event.tenant_id, f"visitor:{visitor_id}")
                if visitor_subject_key in self.hot_state:
                    removed_hot_keys.append(visitor_subject_key)
                    self.hot_state.pop(visitor_subject_key, None)
                self.segment_membership.discard(visitor_subject_key)
                self.identity_links.pop((event.tenant_id, visitor_id), None)
                self.erased_identifiers.add((event.tenant_id, "visitor", visitor_id))
            self.user_to_visitors.pop((event.tenant_id, event.user_id), None)
            self.erased_identifiers.add((event.tenant_id, "user", event.user_id))

        if event.visitor_id:
            linked_user = self.identity_links.pop((event.tenant_id, event.visitor_id), None)
            if linked_user:
                self.user_to_visitors.get((event.tenant_id, linked_user), set()).discard(event.visitor_id)
                self.erased_identifiers.add((event.tenant_id, "user", linked_user))
            self.erased_identifiers.add((event.tenant_id, "visitor", event.visitor_id))

        if subject_key in self.hot_state:
            removed_hot_keys.append(subject_key)
            self.hot_state.pop(subject_key, None)
        self.segment_membership.discard(subject_key)
        self.stats.segment_membership_count = len(self.segment_membership)

        tombstone = {
            "tenant_id": event.tenant_id,
            "subject": subject,
            "policy": self.post_erasure_policy,
            "removed_hot_state_keys": sorted(set(removed_hot_keys)),
            "compaction_command": f"erase tenant={event.tenant_id} subject={subject}",
        }
        self.tombstones.append(tombstone)
        self.stats.erasure_processed_count += 1
        self.stats.tombstones_emitted_count = len(self.tombstones)

    def summary(self) -> dict[str, Any]:
        return {
            "identity_links": {f"{tenant}:{visitor}": user for (tenant, visitor), user in sorted(self.identity_links.items())},
            "identity_conflicts": self.identity_conflicts,
            "segment_membership": sorted(self.segment_membership),
            "fallback_queue_depth": len(self.fallback_queue),
            "hot_state_subject_count": len(self.hot_state),
            "tombstones": self.tombstones,
            "post_erasure_policy": self.post_erasure_policy,
        }


def event(
    tenant: str,
    index: int,
    *,
    event_type: EventType = EventType.PAGE_VIEW,
    visitor: str | None = None,
    user: str | None = None,
    event_id: str | None = None,
    session: str | None = None,
    url: str = "/home",
    timestamp: int = 1_700_000_000,
) -> Event:
    visitor_id = visitor if visitor is not None else f"visitor_{index:04d}"
    session_id = session if session is not None else f"session_{index % 17:02d}"
    properties = {"url": url}
    return Event(
        tenant_id=tenant,
        event_type=event_type,
        timestamp=timestamp + index,
        visitor_id=visitor_id,
        user_id=user,
        event_id=event_id or f"{tenant}-{index}-{event_type.value}",
        session_id=session_id,
        properties=properties,
    )


def _tenant_ids(count: int = 520) -> list[str]:
    return [f"tenant_{number:04d}" for number in range(1, count + 1)]


def _finish(name: str, simulator: PipelineSimulator, notes: dict[str, Any]) -> ScenarioResult:
    simulator.stats.assert_invariants()
    return ScenarioResult(name=name, stats=simulator.stats, summary={**simulator.summary(), **notes})


def scenario_normal_load() -> ScenarioResult:
    sim = PipelineSimulator("normal_load")
    for index, tenant in enumerate(_tenant_ids(), start=1):
        url = "/pricing" if index % 29 == 0 else "/home"
        sim.ingest_event(event(tenant, index, url=url))
    return _finish(
        "normal_load",
        sim,
        {
            "setup_assumptions": "520 tenants, one accepted event each, light pricing-page traffic.",
            "degraded": "nothing intentionally degraded",
            "operator_action": "watch ingest ack p95, Kinesis throttles, Flink lag, and reconciliation deltas",
        },
    )


def scenario_black_friday_spike() -> ScenarioResult:
    sim = PipelineSimulator("black_friday_spike", tenant_rate_limit=10_000)
    tenants = _tenant_ids()
    for round_index in range(4):
        for tenant_index, tenant in enumerate(tenants, start=1):
            idx = round_index * len(tenants) + tenant_index
            url = "/pricing" if tenant_index % 7 == 0 else "/sale"
            sim.ingest_event(event(tenant, idx, url=url))
            if tenant_index % 101 == 0:
                duplicate = event(tenant, idx, url=url)
                sim.ingest_event(duplicate)
    return _finish(
        "black_friday_spike",
        sim,
        {
            "setup_assumptions": "local spike multiplies normal event count and injects browser retries.",
            "degraded": "duplicates are deduped; no stream outage in this scenario",
            "operator_action": "pre-warm/provision stream capacity before launch and raise Kinesis quotas if forecast requires it",
        },
    )


def scenario_stream_outage_with_fallback_replay() -> ScenarioResult:
    sim = PipelineSimulator("stream_outage_with_fallback_replay")
    tenants = _tenant_ids(80)
    for idx, tenant in enumerate(tenants, start=1):
        fallback_available = idx % 17 != 0
        sim.ingest_event(
            event(tenant, idx, url="/pricing" if idx % 5 == 0 else "/home"),
            stream_available=False,
            fallback_available=fallback_available,
        )
    for idx, tenant in enumerate(_tenant_ids(20), start=1000):
        sim.ingest_event(event(tenant, idx, url="/home"), stream_available=True)
    sim.replay_fallback()
    return _finish(
        "stream_outage_with_fallback_replay",
        sim,
        {
            "setup_assumptions": "Kinesis write path is unavailable for the first local batch; fallback is durable except injected write failures.",
            "degraded": "events accepted through fallback wait for replay; fallback-write failures are not acknowledged",
            "operator_action": "page on stream write failures, watch fallback age/depth, replay after stream recovery, reconcile by tenant and event time",
        },
    )


def scenario_largest_tenant_hotspot() -> ScenarioResult:
    sim = PipelineSimulator("largest_tenant_hotspot", tenant_rate_limit=40)
    large_tenant = "tenant_0001"
    for idx in range(1, 90):
        sim.ingest_event(event(large_tenant, idx, visitor=f"hot_visitor_{idx % 3}", url="/pricing"))
    for idx, tenant in enumerate(_tenant_ids(25)[1:], start=500):
        sim.ingest_event(event(tenant, idx, url="/home"))
    return _finish(
        "largest_tenant_hotspot",
        sim,
        {
            "setup_assumptions": "one tenant exceeds local per-tenant limit while neighboring tenants continue.",
            "degraded": "hot tenant is throttled and isolated before stream write",
            "operator_action": "increase tenant bucket count or provision tenant-isolated stream if sustained and commercially justified",
        },
    )


def scenario_gdpr_erasure() -> ScenarioResult:
    sim = PipelineSimulator("gdpr_erasure")
    tenant = "tenant_0099"
    visitor = "visitor_erasure"
    user = "user_erasure"
    sim.ingest_event(event(tenant, 1, visitor=visitor, event_id="pv-1", url="/pricing"))
    sim.ingest_event(event(tenant, 2, visitor=visitor, event_id="pv-2", url="/pricing"))
    sim.ingest_event(event(tenant, 3, visitor=visitor, user=user, event_id="login-1", event_type=EventType.LOGIN, url="/login"))
    sim.ingest_event(event(tenant, 4, visitor=visitor, user=user, event_id="pv-3", url="/pricing"))
    erasure = event(tenant, 5, visitor=visitor, user=user, event_id="erase-1", event_type=EventType.ERASURE_REQUEST, url="/privacy")
    sim.ingest_event(erasure)
    sim.ingest_event(event(tenant, 6, visitor=visitor, user=user, event_id="post-erase-1", url="/pricing"))
    return _finish(
        "gdpr_erasure",
        sim,
        {
            "setup_assumptions": "identified user qualifies for pricing segment, then sends erasure request.",
            "degraded": "post-erasure events for the same identifiers are rejected until new consent policy says otherwise",
            "operator_action": "verify hot-state deletion, emit warehouse/delete manifests, and route legal-retention exceptions for approval",
        },
    )


def scenario_duplicate_and_out_of_order() -> ScenarioResult:
    sim = PipelineSimulator("duplicate_and_out_of_order")
    tenant = "tenant_0200"
    visitor = "visitor_merge"
    sim.ingest_event(event(tenant, 30, visitor=visitor, event_id="pv-old-1", url="/pricing", timestamp=1_700_000_020))
    sim.ingest_event(event(tenant, 10, visitor=visitor, user="user_a", event_id="login-a", event_type=EventType.LOGIN, url="/login", timestamp=1_700_000_010))
    sim.ingest_event(event(tenant, 11, visitor=visitor, event_id="pv-old-2", url="/pricing", timestamp=1_700_000_011))
    duplicate = event(tenant, 12, visitor=visitor, event_id="pv-dup", url="/pricing", timestamp=1_700_000_012)
    sim.ingest_event(duplicate)
    sim.ingest_event(duplicate)
    sim.ingest_event(event(tenant, 13, visitor=visitor, user="user_b", event_id="login-b", event_type=EventType.LOGIN, url="/login", timestamp=1_700_000_013))
    return _finish(
        "duplicate_and_out_of_order",
        sim,
        {
            "setup_assumptions": "behavior arrives before and after login, one duplicate retry, then a conflicting login.",
            "degraded": "conflicting identity is recorded and the first mapping remains canonical",
            "operator_action": "send deterministic conflict record to review queue and reconcile downstream facts by canonical subject",
        },
    )


SCENARIOS: dict[str, Callable[[], ScenarioResult]] = {
    "normal_load": scenario_normal_load,
    "black_friday_spike": scenario_black_friday_spike,
    "stream_outage_with_fallback_replay": scenario_stream_outage_with_fallback_replay,
    "largest_tenant_hotspot": scenario_largest_tenant_hotspot,
    "gdpr_erasure": scenario_gdpr_erasure,
    "duplicate_and_out_of_order": scenario_duplicate_and_out_of_order,
}


def run_scenario(name: str) -> ScenarioResult:
    if name not in SCENARIOS:
        known = ", ".join(sorted(SCENARIOS))
        raise ValueError(f"unknown scenario {name!r}; known scenarios: {known}")
    return SCENARIOS[name]()


def run_all_scenarios() -> list[ScenarioResult]:
    return [run_scenario(name) for name in SCENARIOS]


def write_scenario_outputs(results: Iterable[ScenarioResult], output_dir: Path = EVIDENCE_SCENARIO_DIR) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for result in results:
        json_path = output_dir / f"{result.name}.json"
        md_path = output_dir / f"{result.name}.md"
        json_path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        md_path.write_text(_scenario_markdown(result), encoding="utf-8")
        written.extend([json_path, md_path])
    return written


def _scenario_markdown(result: ScenarioResult) -> str:
    stats = result.stats.to_dict()
    summary = result.summary
    key_counters = stats.copy()
    key_counters.pop("tenant_throttles", None)
    key_counters.pop("lifecycle_counts", None)
    key_counters.pop("invariant", None)
    lines = [
        f"# Scenario: {result.name}",
        "",
        "## Setup assumptions",
        str(summary.get("setup_assumptions", "")),
        "",
        "## What failed or degraded",
        str(summary.get("degraded", "")),
        "",
        "## Key counters",
        "",
        "```json",
        json.dumps(key_counters, indent=2, sort_keys=True),
        "```",
        "",
        "## Invariant result",
        f"accepted_count == processed_unique_count + duplicate_deduped_count + erased_or_tombstoned_count + pending_replay_count: {stats['invariant']['holds']}",
        f"lost_after_accept: {stats['lost_after_accept_count']}",
        "",
        "## What this does not prove",
        "This is a deterministic semantic simulation. It does not prove AWS throughput, regional quota readiness, browser delivery, or ClickHouse query latency.",
        "",
        "## Operator action if this happened in production",
        str(summary.get("operator_action", "")),
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["all", *SCENARIOS.keys()], default="all")
    parser.add_argument("--write-evidence", action="store_true")
    args = parser.parse_args()

    results = run_all_scenarios() if args.scenario == "all" else [run_scenario(args.scenario)]
    for result in results:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    if args.write_evidence:
        paths = write_scenario_outputs(results)
        for path in paths:
            print(f"wrote {path}")


if __name__ == "__main__":
    main()

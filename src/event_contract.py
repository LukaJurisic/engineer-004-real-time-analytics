from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
from typing import Any


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    FORM_SUBMIT = "form_submit"
    IDENTIFY = "identify"
    LOGIN = "login"
    CUSTOM_EVENT = "custom_event"
    ERASURE_REQUEST = "erasure_request"


class EventLifecycleState(str, Enum):
    RECEIVED = "received"
    REJECTED_INVALID = "rejected_invalid"
    STREAM_WRITE_FAILED = "stream_write_failed"
    FALLBACK_WRITE_FAILED = "fallback_write_failed"
    NOT_ACCEPTED = "not_accepted"
    FALLBACK_DURABLE = "fallback_durable"
    STREAM_DURABLE = "stream_durable"
    ACCEPTED = "accepted"
    PROCESSED = "processed"
    DUPLICATE_DEDUPED = "duplicate_deduped"
    ERASED_OR_TOMBSTONED = "erased_or_tombstoned"
    PENDING_REPLAY = "pending_replay"


VALID_EVENT_TYPES = {item.value for item in EventType}


@dataclass(frozen=True)
class Event:
    tenant_id: str
    event_type: EventType | str
    timestamp: int
    visitor_id: str | None = None
    user_id: str | None = None
    event_id: str | None = None
    session_id: str | None = None
    properties: dict[str, Any] = field(default_factory=dict)
    received_at: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.event_type, EventType):
            object.__setattr__(self, "event_type", EventType(str(self.event_type)))
        if self.received_at is None:
            object.__setattr__(self, "received_at", self.timestamp)

    @property
    def dedupe_key(self) -> str:
        if self.event_id:
            return f"{self.tenant_id}:event_id:{self.event_id}"

        identity = self.user_id or self.visitor_id or self.session_id or "unknown"
        timestamp_bucket = self.timestamp // 5
        stable_payload = {
            "tenant_id": self.tenant_id,
            "identity": identity,
            "event_type": self.event_type.value,
            "timestamp_bucket": timestamp_bucket,
            "properties": self.properties,
        }
        encoded = json.dumps(stable_payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]
        return f"{self.tenant_id}:fingerprint:{digest}"

    def partition_key(self, tenant_bucket_count: int = 16) -> str:
        identity = self.visitor_id or self.session_id or self.user_id or "anonymous"
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        bucket = int(digest[:8], 16) % tenant_bucket_count
        return f"{self.tenant_id}:{bucket}"

    def subject_identifiers(self) -> set[tuple[str, str, str]]:
        identifiers: set[tuple[str, str, str]] = set()
        if self.user_id:
            identifiers.add((self.tenant_id, "user", self.user_id))
        if self.visitor_id:
            identifiers.add((self.tenant_id, "visitor", self.visitor_id))
        if self.session_id:
            identifiers.add((self.tenant_id, "session", self.session_id))
        return identifiers


@dataclass
class PipelineStats:
    scenario: str
    browser_generated_count: int = 0
    received_count: int = 0
    rejected_invalid_count: int = 0
    accepted_count: int = 0
    stream_write_failed_count: int = 0
    fallback_write_failed_count: int = 0
    not_accepted_count: int = 0
    fallback_used_count: int = 0
    processed_unique_count: int = 0
    duplicate_deduped_count: int = 0
    erased_or_tombstoned_count: int = 0
    pending_replay_count: int = 0
    lost_after_accept_count: int = 0
    erasure_processed_count: int = 0
    tombstones_emitted_count: int = 0
    identity_conflict_count: int = 0
    segment_membership_count: int = 0
    tenant_throttles: dict[str, int] = field(default_factory=dict)
    lifecycle_counts: dict[str, int] = field(default_factory=dict)

    def record_lifecycle(self, state: EventLifecycleState) -> None:
        self.lifecycle_counts[state.value] = self.lifecycle_counts.get(state.value, 0) + 1

    @property
    def invariant_left(self) -> int:
        return self.accepted_count

    @property
    def invariant_right(self) -> int:
        return (
            self.processed_unique_count
            + self.duplicate_deduped_count
            + self.erased_or_tombstoned_count
            + self.pending_replay_count
        )

    @property
    def invariant_holds(self) -> bool:
        return self.invariant_left == self.invariant_right and self.lost_after_accept_count == 0

    def assert_invariants(self) -> None:
        if not self.invariant_holds:
            raise AssertionError(
                "accepted_count invariant failed: "
                f"{self.invariant_left} != {self.invariant_right}; "
                f"lost_after_accept={self.lost_after_accept_count}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "browser_generated_count": self.browser_generated_count,
            "received_count": self.received_count,
            "rejected_invalid_count": self.rejected_invalid_count,
            "accepted_count": self.accepted_count,
            "stream_write_failed_count": self.stream_write_failed_count,
            "fallback_write_failed_count": self.fallback_write_failed_count,
            "not_accepted_count": self.not_accepted_count,
            "fallback_used_count": self.fallback_used_count,
            "processed_unique_count": self.processed_unique_count,
            "duplicate_deduped_count": self.duplicate_deduped_count,
            "erased_or_tombstoned_count": self.erased_or_tombstoned_count,
            "pending_replay_count": self.pending_replay_count,
            "lost_after_accept_count": self.lost_after_accept_count,
            "erasure_processed_count": self.erasure_processed_count,
            "tombstones_emitted_count": self.tombstones_emitted_count,
            "identity_conflict_count": self.identity_conflict_count,
            "segment_membership_count": self.segment_membership_count,
            "tenant_throttles": dict(sorted(self.tenant_throttles.items())),
            "lifecycle_counts": dict(sorted(self.lifecycle_counts.items())),
            "invariant": {
                "accepted_count": self.invariant_left,
                "processed_unique_plus_duplicate_plus_erased_plus_pending": self.invariant_right,
                "lost_after_accept": self.lost_after_accept_count,
                "holds": self.invariant_holds,
            },
        }

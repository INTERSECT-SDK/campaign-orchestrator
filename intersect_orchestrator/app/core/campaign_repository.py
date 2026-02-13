"""Repository interface for storing campaign state changes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from typing import Iterable, Protocol
from uuid import UUID

from pydantic import BaseModel

from ..api.v1.endpoints.orchestrator.models.campaign import Campaign
from ..api.v1.endpoints.orchestrator.models.campaign_state import CampaignState


class CampaignEvent(BaseModel):
    """Event describing a campaign state change."""

    event_id: UUID
    campaign_id: UUID
    seq: int
    event_type: str
    payload: dict[str, object]
    timestamp: datetime


class CampaignSnapshot(BaseModel):
    """Snapshot of campaign state."""

    campaign_id: UUID
    version: int
    state: CampaignState
    updated_at: datetime


class CampaignRepository(Protocol):
    """Repository contract for campaign storage backends."""

    def create_campaign(self, campaign_id: UUID, campaign: Campaign, state: CampaignState) -> None:
        ...

    def get_campaign(self, campaign_id: UUID) -> Campaign | None:
        ...

    def append_event(self, event: CampaignEvent, *, expected_version: int) -> None:
        ...

    def load_events(self, campaign_id: UUID, *, after_seq: int = 0) -> Iterable[CampaignEvent]:
        ...

    def load_snapshot(self, campaign_id: UUID) -> CampaignSnapshot | None:
        ...

    def update_snapshot(self, snapshot: CampaignSnapshot, *, expected_version: int) -> None:
        ...

    def campaign_exists(self, campaign_id: UUID) -> bool:
        ...


@dataclass
class InMemoryCampaignRepository:
    """In-memory campaign repository with optimistic locking."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._campaigns: dict[UUID, Campaign] = {}
        self._snapshots: dict[UUID, CampaignSnapshot] = {}
        self._events: dict[UUID, list[CampaignEvent]] = {}

    def create_campaign(self, campaign_id: UUID, campaign: Campaign, state: CampaignState) -> None:
        now = datetime.now(UTC)
        with self._lock:
            if campaign_id in self._campaigns:
                msg = f'Campaign already exists: {campaign_id}'
                raise ValueError(msg)
            snapshot = CampaignSnapshot(
                campaign_id=campaign_id,
                version=0,
                state=state,
                updated_at=now,
            )
            self._campaigns[campaign_id] = campaign
            self._snapshots[campaign_id] = snapshot
            self._events[campaign_id] = []

    def get_campaign(self, campaign_id: UUID) -> Campaign | None:
        with self._lock:
            return self._campaigns.get(campaign_id)

    def append_event(self, event: CampaignEvent, *, expected_version: int) -> None:
        with self._lock:
            snapshot = self._snapshots.get(event.campaign_id)
            if snapshot is None:
                msg = f'Campaign not found: {event.campaign_id}'
                raise ValueError(msg)
            if snapshot.version != expected_version:
                msg = 'version mismatch when appending event'
                raise ValueError(msg)
            if event.seq != expected_version + 1:
                msg = 'sequence mismatch when appending event'
                raise ValueError(msg)
            self._events[event.campaign_id].append(event)

    def load_events(self, campaign_id: UUID, *, after_seq: int = 0) -> Iterable[CampaignEvent]:
        with self._lock:
            events = self._events.get(campaign_id, [])
            return [event for event in events if event.seq > after_seq]

    def load_snapshot(self, campaign_id: UUID) -> CampaignSnapshot | None:
        with self._lock:
            snapshot = self._snapshots.get(campaign_id)
            if snapshot is None:
                return None
            return snapshot.model_copy(deep=True)

    def update_snapshot(self, snapshot: CampaignSnapshot, *, expected_version: int) -> None:
        with self._lock:
            current = self._snapshots.get(snapshot.campaign_id)
            if current is None:
                msg = f'Campaign not found: {snapshot.campaign_id}'
                raise ValueError(msg)
            if current.version != expected_version:
                msg = 'version mismatch when updating snapshot'
                raise ValueError(msg)
            self._snapshots[snapshot.campaign_id] = snapshot

    def campaign_exists(self, campaign_id: UUID) -> bool:
        with self._lock:
            return campaign_id in self._campaigns

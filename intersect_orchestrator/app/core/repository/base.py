"""Repository interface and shared models for campaign storage."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Protocol
from uuid import UUID

from pydantic import BaseModel

from ...api.v1.endpoints.orchestrator.models.campaign import Campaign
from ...api.v1.endpoints.orchestrator.models.campaign_state import CampaignState


def _require_pymongo() -> tuple[object, object]:
    try:
        from pymongo import ASCENDING, MongoClient
    except ImportError as exc:
        raise ImportError('pymongo is required to use MongoCampaignRepository') from exc
    return MongoClient, ASCENDING


def _require_psycopg() -> object:
    try:
        from psycopg.types.json import Json
    except ImportError as exc:
        raise ImportError('psycopg is required to use PostgresCampaignRepository') from exc
    return Json


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

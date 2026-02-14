"""MongoDB campaign repository implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from ...api.v1.endpoints.orchestrator.models.campaign import Campaign
from ...api.v1.endpoints.orchestrator.models.campaign_state import CampaignState
from .base import CampaignEvent, CampaignSnapshot, require_pymongo


@dataclass
class MongoCampaignRepository:
    """MongoDB-backed campaign repository with optimistic locking."""

    def __init__(self, client: Any, db_name: str = 'intersect_orchestrator') -> None:
        _, ascending = require_pymongo()
        self._ascending = ascending
        self._client = client
        self._db = client[db_name]
        self._campaigns = self._db['campaigns']
        self._snapshots = self._db['snapshots']
        self._events = self._db['events']

        self._campaigns.create_index([('campaign_id', ascending)], unique=True)
        self._snapshots.create_index([('campaign_id', ascending)], unique=True)
        self._events.create_index([('campaign_id', ascending), ('seq', ascending)], unique=True)

    def create_campaign(self, campaign_id: UUID, campaign: Campaign, state: CampaignState) -> None:
        now = datetime.now(UTC)
        campaign_key = str(campaign_id)

        if self._campaigns.find_one({'campaign_id': campaign_key}) is not None:
            msg = f'Campaign already exists: {campaign_id}'
            raise ValueError(msg)

        self._campaigns.insert_one(
            {
                'campaign_id': campaign_key,
                'campaign': campaign.model_dump(by_alias=True),
            }
        )
        self._snapshots.insert_one(
            {
                'campaign_id': campaign_key,
                'version': 0,
                'state': state.model_dump(by_alias=True),
                'updated_at': now,
            }
        )

    def get_campaign(self, campaign_id: UUID) -> Campaign | None:
        doc = self._campaigns.find_one({'campaign_id': str(campaign_id)})
        if doc is None:
            return None
        return Campaign.model_validate(doc['campaign'])

    def append_event(self, event: CampaignEvent, *, expected_version: int) -> None:
        snapshot = self._snapshots.find_one({'campaign_id': str(event.campaign_id)})
        if snapshot is None:
            msg = f'Campaign not found: {event.campaign_id}'
            raise ValueError(msg)
        if snapshot['version'] != expected_version:
            msg = 'version mismatch when appending event'
            raise ValueError(msg)
        if event.seq != expected_version + 1:
            msg = 'sequence mismatch when appending event'
            raise ValueError(msg)

        self._events.insert_one(
            {
                'event_id': str(event.event_id),
                'campaign_id': str(event.campaign_id),
                'seq': event.seq,
                'event_type': event.event_type,
                'payload': event.payload,
                'timestamp': event.timestamp,
            }
        )

    def load_events(self, campaign_id: UUID, *, after_seq: int = 0) -> list[CampaignEvent]:
        cursor = self._events.find(
            {'campaign_id': str(campaign_id), 'seq': {'$gt': after_seq}}
        ).sort('seq', self._ascending)
        return [
            CampaignEvent(
                event_id=UUID(doc['event_id']),
                campaign_id=UUID(doc['campaign_id']),
                seq=doc['seq'],
                event_type=doc['event_type'],
                payload=doc['payload'],
                timestamp=doc['timestamp'],
            )
            for doc in cursor
        ]

    def load_snapshot(self, campaign_id: UUID) -> CampaignSnapshot | None:
        doc = self._snapshots.find_one({'campaign_id': str(campaign_id)})
        if doc is None:
            return None
        return CampaignSnapshot(
            campaign_id=UUID(doc['campaign_id']),
            version=doc['version'],
            state=CampaignState.model_validate(doc['state']),
            updated_at=doc['updated_at'],
        )

    def update_snapshot(self, snapshot: CampaignSnapshot, *, expected_version: int) -> None:
        result = self._snapshots.update_one(
            {'campaign_id': str(snapshot.campaign_id), 'version': expected_version},
            {
                '$set': {
                    'version': snapshot.version,
                    'state': snapshot.state.model_dump(by_alias=True),
                    'updated_at': snapshot.updated_at,
                }
            },
        )
        if result.matched_count == 0:
            msg = 'version mismatch when updating snapshot'
            raise ValueError(msg)

    def campaign_exists(self, campaign_id: UUID) -> bool:
        return self._campaigns.find_one({'campaign_id': str(campaign_id)}) is not None

"""PostgreSQL campaign repository implementation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from ...api.v1.endpoints.orchestrator.models.campaign import Campaign
from ...api.v1.endpoints.orchestrator.models.campaign_state import CampaignState
from . import base as repository_base
from .base import CampaignEvent, CampaignSnapshot


@dataclass
class PostgresCampaignRepository:
    """PostgreSQL-backed campaign repository with optimistic locking."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self._json = repository_base._require_psycopg()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS campaigns (
                    campaign_id UUID PRIMARY KEY,
                    campaign JSONB NOT NULL
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    campaign_id UUID PRIMARY KEY,
                    version INTEGER NOT NULL,
                    state JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id UUID PRIMARY KEY,
                    campaign_id UUID NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    UNIQUE (campaign_id, seq)
                );
                """
            )
        self._connection.commit()

    def create_campaign(self, campaign_id: UUID, campaign: Campaign, state: CampaignState) -> None:
        now = datetime.now(UTC)
        with self._connection.transaction():
            cursor = self._connection.execute(
                'SELECT 1 FROM campaigns WHERE campaign_id = %s',
                (campaign_id,),
            )
            if cursor.fetchone() is not None:
                msg = f'Campaign already exists: {campaign_id}'
                raise ValueError(msg)

            self._connection.execute(
                'INSERT INTO campaigns (campaign_id, campaign) VALUES (%s, %s)',
                (campaign_id, self._json(campaign.model_dump(by_alias=True))),
            )
            self._connection.execute(
                'INSERT INTO snapshots (campaign_id, version, state, updated_at) '
                'VALUES (%s, %s, %s, %s)',
                (campaign_id, 0, self._json(state.model_dump(by_alias=True)), now),
            )

    def get_campaign(self, campaign_id: UUID) -> Campaign | None:
        cursor = self._connection.execute(
            'SELECT campaign FROM campaigns WHERE campaign_id = %s',
            (campaign_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return Campaign.model_validate(row[0])

    def append_event(self, event: CampaignEvent, *, expected_version: int) -> None:
        with self._connection.transaction():
            cursor = self._connection.execute(
                'SELECT version FROM snapshots WHERE campaign_id = %s FOR UPDATE',
                (event.campaign_id,),
            )
            row = cursor.fetchone()
            if row is None:
                msg = f'Campaign not found: {event.campaign_id}'
                raise ValueError(msg)
            if row[0] != expected_version:
                msg = 'version mismatch when appending event'
                raise ValueError(msg)
            if event.seq != expected_version + 1:
                msg = 'sequence mismatch when appending event'
                raise ValueError(msg)

            self._connection.execute(
                'INSERT INTO events (event_id, campaign_id, seq, event_type, payload, timestamp) '
                'VALUES (%s, %s, %s, %s, %s, %s)',
                (
                    event.event_id,
                    event.campaign_id,
                    event.seq,
                    event.event_type,
                    self._json(event.payload),
                    event.timestamp,
                ),
            )

    def load_events(self, campaign_id: UUID, *, after_seq: int = 0) -> list[CampaignEvent]:
        cursor = self._connection.execute(
            'SELECT event_id, campaign_id, seq, event_type, payload, timestamp '
            'FROM events WHERE campaign_id = %s AND seq > %s ORDER BY seq',
            (campaign_id, after_seq),
        )
        return [
            CampaignEvent(
                event_id=row[0],
                campaign_id=row[1],
                seq=row[2],
                event_type=row[3],
                payload=row[4],
                timestamp=row[5],
            )
            for row in cursor.fetchall()
        ]

    def load_snapshot(self, campaign_id: UUID) -> CampaignSnapshot | None:
        cursor = self._connection.execute(
            'SELECT version, state, updated_at FROM snapshots WHERE campaign_id = %s',
            (campaign_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return CampaignSnapshot(
            campaign_id=campaign_id,
            version=row[0],
            state=CampaignState.model_validate(row[1]),
            updated_at=row[2],
        )

    def update_snapshot(self, snapshot: CampaignSnapshot, *, expected_version: int) -> None:
        cursor = self._connection.execute(
            'UPDATE snapshots SET version = %s, state = %s, updated_at = %s '
            'WHERE campaign_id = %s AND version = %s',
            (
                snapshot.version,
                self._json(snapshot.state.model_dump(by_alias=True)),
                snapshot.updated_at,
                snapshot.campaign_id,
                expected_version,
            ),
        )
        if cursor.rowcount == 0:
            msg = 'version mismatch when updating snapshot'
            raise ValueError(msg)

    def campaign_exists(self, campaign_id: UUID) -> bool:
        cursor = self._connection.execute(
            'SELECT 1 FROM campaigns WHERE campaign_id = %s',
            (campaign_id,),
        )
        return cursor.fetchone() is not None

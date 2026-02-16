from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
    CampaignState,
    ExecutionStatus,
)
from intersect_orchestrator.app.core.repository import CampaignEvent, MongoCampaignRepository

if TYPE_CHECKING:
    from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
        Campaign,
    )

# Use fixtures from conftest.py:
# - mongo_uri
# - mongo_db
# - simple_campaign
# - repository_mongo (aliased below as repository)


@pytest.fixture
def repository(repository_mongo: MongoCampaignRepository) -> MongoCampaignRepository:
    """Alias repository_mongo fixture for backward compatibility."""
    return repository_mongo


def test_mongo_repository_integration(
    repository: MongoCampaignRepository, simple_campaign: Campaign
) -> None:
    campaign_id = uuid4()

    state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
    repository.create_campaign(campaign_id, simple_campaign, state)

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot is not None
    assert snapshot.version == 0

    event = CampaignEvent(
        event_id=uuid4(),
        campaign_id=campaign_id,
        seq=1,
        event_type='STATUS_CHANGED',
        payload={'from': 'queued', 'to': 'running'},
        timestamp=datetime.now(UTC),
    )
    repository.append_event(event, expected_version=0)

    snapshot.state.status = ExecutionStatus.RUNNING
    snapshot.version = event.seq
    snapshot.updated_at = event.timestamp
    repository.update_snapshot(snapshot, expected_version=0)

    reloaded = repository.load_snapshot(campaign_id)
    assert reloaded is not None
    assert reloaded.version == 1
    assert reloaded.state.status == ExecutionStatus.RUNNING

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

try:
    import mongomock
except ImportError:
    pytest.skip('mongomock not available; skipping mongo unit tests', allow_module_level=True)

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    Task,
    TaskGroup,
)
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
    CampaignState,
    ExecutionStatus,
)
from intersect_orchestrator.app.core.repository import CampaignEvent, MongoCampaignRepository


@pytest.fixture
def simple_campaign() -> Campaign:
    return Campaign(
        id='campaign-repo-1',
        name='Repo Campaign',
        user='tester',
        description='Repo campaign description',
        task_groups=[
            TaskGroup(
                id='tg-1',
                group_dependencies=[],
                tasks=[
                    Task(
                        id='task-1',
                        hierarchy='capability',
                        capability='capability-1',
                        operation_id='op-1',
                    )
                ],
            )
        ],
    )


@pytest.fixture
def repository() -> MongoCampaignRepository:
    client = mongomock.MongoClient()
    return MongoCampaignRepository(client, db_name='test_campaign_repo')


def test_mongo_repository_create_and_load_snapshot(
    repository: MongoCampaignRepository, simple_campaign: Campaign
) -> None:
    campaign_id = uuid4()

    state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
    repository.create_campaign(campaign_id, simple_campaign, state)

    assert repository.campaign_exists(campaign_id)
    assert repository.get_campaign(campaign_id) == simple_campaign

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot is not None
    assert snapshot.version == 0
    assert snapshot.state.status == ExecutionStatus.QUEUED

    assert list(repository.load_events(campaign_id)) == []


def test_mongo_repository_append_event_and_update_snapshot(
    repository: MongoCampaignRepository, simple_campaign: Campaign
) -> None:
    campaign_id = uuid4()

    state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
    repository.create_campaign(campaign_id, simple_campaign, state)

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot is not None

    event = CampaignEvent(
        event_id=uuid4(),
        campaign_id=campaign_id,
        seq=snapshot.version + 1,
        event_type='STATUS_CHANGED',
        payload={'from': 'queued', 'to': 'running'},
        timestamp=datetime.now(UTC),
    )

    repository.append_event(event, expected_version=snapshot.version)

    snapshot.state.status = ExecutionStatus.RUNNING
    snapshot.version = event.seq
    snapshot.updated_at = event.timestamp

    repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    reloaded = repository.load_snapshot(campaign_id)
    assert reloaded is not None
    assert reloaded.version == 1
    assert reloaded.state.status == ExecutionStatus.RUNNING
    assert [e.seq for e in repository.load_events(campaign_id)] == [1]


def test_mongo_repository_optimistic_locking(
    repository: MongoCampaignRepository, simple_campaign: Campaign
) -> None:
    campaign_id = uuid4()

    state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
    repository.create_campaign(campaign_id, simple_campaign, state)

    event = CampaignEvent(
        event_id=uuid4(),
        campaign_id=campaign_id,
        seq=1,
        event_type='STATUS_CHANGED',
        payload={'from': 'queued', 'to': 'running'},
        timestamp=datetime.now(UTC),
    )

    with pytest.raises(ValueError, match='version mismatch'):
        repository.append_event(event, expected_version=2)

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import pytest

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


@pytest.fixture(scope='session')
def mongo_uri() -> str:
    return os.getenv(
        'CAMPAIGN_REPOSITORY_MONGO_URI',
        'mongodb://intersect:intersect@localhost:27017/?authSource=admin',
    )


@pytest.fixture(scope='session')
def mongo_db() -> str:
    return os.getenv('CAMPAIGN_REPOSITORY_MONGO_DB', 'intersect_orchestrator')


def _wait_for_mongo(uri: str, timeout: float = 10.0) -> None:
    import pymongo

    start = time.time()
    while time.time() - start < timeout:
        try:
            client = pymongo.MongoClient(uri)
            client.admin.command('ping')
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
        else:
            return
    pytest.skip('MongoDB not available for integration tests')


@pytest.fixture
def repository(mongo_uri: str, mongo_db: str) -> MongoCampaignRepository:
    _wait_for_mongo(mongo_uri)
    import pymongo

    client = pymongo.MongoClient(mongo_uri)
    return MongoCampaignRepository(client, db_name=mongo_db)


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

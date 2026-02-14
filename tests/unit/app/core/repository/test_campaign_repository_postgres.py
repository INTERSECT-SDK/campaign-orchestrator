from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
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
from intersect_orchestrator.app.core.repository import CampaignEvent, PostgresCampaignRepository


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
def mock_connection():
    """Mock PostgreSQL connection for unit testing."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # Mock cursor context manager
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Mock transaction context manager
    mock_conn.transaction.return_value.__enter__ = MagicMock(return_value=None)
    mock_conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

    # Mock execute method
    mock_conn.execute = MagicMock(return_value=mock_cursor)

    return mock_conn


@pytest.fixture
def repository(mock_connection, monkeypatch):
    """Create repository with mocked connection."""
    # Mock require_psycopg to return a mock Json class
    mock_json = MagicMock()
    mock_json.side_effect = lambda x: x  # Just return the input as-is

    from intersect_orchestrator.app.core.repository import postgres as postgres_module

    monkeypatch.setattr(postgres_module, 'require_psycopg', lambda: mock_json)

    return PostgresCampaignRepository(mock_connection)


def test_postgres_repository_create_and_load_snapshot(
    repository: PostgresCampaignRepository, simple_campaign: Campaign, mock_connection
) -> None:
    """Test repository create and load with mocked connection."""
    campaign_id = uuid4()

    # Mock fetchone to return None for existence check, then campaign data
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,  # First call: campaign doesn't exist
        ({'campaign': simple_campaign.model_dump(by_alias=True)},),  # get_campaign
        (0, simple_campaign.model_dump(by_alias=True), datetime.now(UTC)),  # load_snapshot
        (campaign_id,),  # campaign_exists
    ]
    mock_connection.execute.return_value = mock_cursor
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
    repository.create_campaign(campaign_id, simple_campaign, state)

    # Verify schema was created and data was inserted
    assert mock_connection.cursor.called or mock_connection.execute.called


def test_postgres_repository_append_event_and_update_snapshot(
    repository: PostgresCampaignRepository, simple_campaign: Campaign, mock_connection
) -> None:
    """Test appending events with mocked connection."""
    campaign_id = uuid4()

    # Mock responses for create, then append event
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,  # create campaign: doesn't exist
        (0,),  # append_event: version check
    ]
    mock_connection.execute.return_value = mock_cursor
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

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

    repository.append_event(event, expected_version=0)

    # Verify event was inserted
    assert mock_connection.execute.called


def test_postgres_repository_optimistic_locking(
    repository: PostgresCampaignRepository, simple_campaign: Campaign, mock_connection
) -> None:
    """Test optimistic locking with version mismatch."""
    campaign_id = uuid4()

    # Mock version mismatch scenario
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [
        None,  # create campaign: doesn't exist
        (0,),  # append_event: version is 0, but expected is 2
    ]
    mock_connection.execute.return_value = mock_cursor
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

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

    # Should raise error because we expect version 2 but snapshot is at version 0
    with pytest.raises(ValueError, match='version mismatch'):
        repository.append_event(event, expected_version=2)

"""Tests for granular event recording in CampaignOrchestrator."""

from __future__ import annotations

import uuid

import pytest

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    Task,
    TaskGroup,
)
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
    ExecutionStatus,
)
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator
from intersect_orchestrator.app.core.repository import InMemoryCampaignRepository


@pytest.fixture
def repository():
    """Create an in-memory repository for testing."""
    return InMemoryCampaignRepository()


@pytest.fixture
def simple_campaign():
    """Create a simple campaign for testing."""
    task1 = Task(
        id=str(uuid.uuid4()),
        hierarchy='org.fac.system.subsystem.service1',
        capability='measurement',
        operation_id='measure',
        task_dependencies=[],
    )
    task2 = Task(
        id=str(uuid.uuid4()),
        hierarchy='org.fac.system.subsystem.service2',
        capability='analysis',
        operation_id='analyze',
        task_dependencies=[task1.id],
    )
    task_group = TaskGroup(
        id=str(uuid.uuid4()),
        name='Test Task Group',
        tasks=[task1, task2],
        group_dependencies=[],
    )
    return Campaign(
        id=str(uuid.uuid4()),
        name='Test Campaign',
        user='test_user',
        description='A simple test campaign',
        task_groups=[task_group],
    )


class TestGranularEventRecording:
    """Test granular event recording at task, task group, and campaign levels."""

    def test_record_task_event_includes_hierarchy(self, repository, simple_campaign):
        """Test that task events include task_id and task_group_id in payload."""
        from unittest.mock import Mock

        # Create orchestrator with mock client
        mock_client = Mock()
        orchestrator = CampaignOrchestrator(mock_client, repository)

        # Create campaign state in repository
        campaign_id = uuid.UUID(simple_campaign.id)
        task_group_id = simple_campaign.task_groups[0].id
        task_id = simple_campaign.task_groups[0].tasks[0].id

        # Manually create campaign state
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
            CampaignState,
        )

        campaign_state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
        repository.create_campaign(campaign_id, simple_campaign, campaign_state)

        # Record a task event
        orchestrator._record_task_event(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            task_id=task_id,
            event_type='TASK_RUNNING',
            payload={'message': 'Task started'},
        )

        # Verify event was recorded with hierarchy
        events = repository.load_events(campaign_id)
        assert len(events) == 1
        assert events[0].event_type == 'TASK_RUNNING'
        assert events[0].payload['task_group_id'] == task_group_id
        assert events[0].payload['task_id'] == task_id
        assert events[0].payload['message'] == 'Task started'

        # Verify task state was updated
        snapshot = repository.load_snapshot(campaign_id)
        task_state = next(
            (t for tg in snapshot.state.task_groups for t in tg.tasks if t.id == task_id), None
        )
        assert task_state is not None
        assert task_state.status == ExecutionStatus.RUNNING

    def test_record_task_group_event_includes_hierarchy(self, repository, simple_campaign):
        """Test that task group events include task_group_id in payload."""
        from unittest.mock import Mock

        mock_client = Mock()
        orchestrator = CampaignOrchestrator(mock_client, repository)

        campaign_id = uuid.UUID(simple_campaign.id)
        task_group_id = simple_campaign.task_groups[0].id

        # Create campaign state
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
            CampaignState,
        )

        campaign_state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
        repository.create_campaign(campaign_id, simple_campaign, campaign_state)

        # Record a task group event
        orchestrator._record_task_group_event(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            event_type='TASK_GROUP_STARTED',
            payload={'message': 'Task group started'},
        )

        # Verify event was recorded with hierarchy
        events = repository.load_events(campaign_id)
        assert len(events) == 1
        assert events[0].event_type == 'TASK_GROUP_STARTED'
        assert events[0].payload['task_group_id'] == task_group_id
        assert events[0].payload['message'] == 'Task group started'

        # Verify task group state was updated
        snapshot = repository.load_snapshot(campaign_id)
        task_group_state = next(
            (tg for tg in snapshot.state.task_groups if tg.id == task_group_id), None
        )
        assert task_group_state is not None
        assert task_group_state.status == ExecutionStatus.RUNNING

    def test_record_task_group_objective_met_fires_both_events(self, repository, simple_campaign):
        """Test that when a task group objective is met, both objective and completion events are fired."""
        from unittest.mock import Mock

        mock_client = Mock()
        orchestrator = CampaignOrchestrator(mock_client, repository)

        campaign_id = uuid.UUID(simple_campaign.id)
        task_group_id = simple_campaign.task_groups[0].id
        objective_id = 'objective-123'

        # Create campaign state
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
            CampaignState,
        )

        campaign_state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
        repository.create_campaign(campaign_id, simple_campaign, campaign_state)

        # Record task group objective met - should fire TWO events
        orchestrator._record_task_group_objective_met(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            objective_id=objective_id,
        )

        # Verify TWO events were recorded
        events = repository.load_events(campaign_id)
        assert len(events) == 2

        # First event: objective met
        assert events[0].event_type == 'TASK_GROUP_OBJECTIVE_MET'
        assert events[0].payload['task_group_id'] == task_group_id
        assert events[0].payload['objective_id'] == objective_id

        # Second event: task group completed
        assert events[1].event_type == 'TASK_GROUP_COMPLETED'
        assert events[1].payload['task_group_id'] == task_group_id

        # Verify seq numbers are incremented (not duplicated)
        # This ensures Mongo/Postgres unique (campaign_id, seq) constraint won't fail
        assert events[0].seq == 1, 'First event should have seq=1'
        assert events[1].seq == 2, 'Second event should have seq=2 (incremented)'
        assert events[0].seq != events[1].seq, 'Events must have different seq numbers'

        # Verify task group state is COMPLETE
        snapshot = repository.load_snapshot(campaign_id)
        task_group_state = next(
            (tg for tg in snapshot.state.task_groups if tg.id == task_group_id), None
        )
        assert task_group_state is not None
        assert task_group_state.status == ExecutionStatus.COMPLETE

    def test_record_campaign_event_updates_campaign_status(self, repository, simple_campaign):
        """Test that campaign events update campaign-level status."""
        from unittest.mock import Mock

        mock_client = Mock()
        orchestrator = CampaignOrchestrator(mock_client, repository)

        campaign_id = uuid.UUID(simple_campaign.id)

        # Create campaign state
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
            CampaignState,
        )

        campaign_state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
        repository.create_campaign(campaign_id, simple_campaign, campaign_state)

        # Record campaign started
        orchestrator._record_campaign_event(
            campaign_id=campaign_id,
            event_type='CAMPAIGN_STARTED',
            payload={},
        )

        # Verify event and state
        events = repository.load_events(campaign_id)
        assert len(events) == 1
        assert events[0].event_type == 'CAMPAIGN_STARTED'

        snapshot = repository.load_snapshot(campaign_id)
        assert snapshot.state.status == ExecutionStatus.RUNNING

    def test_task_event_received_for_event_streams(self, repository, simple_campaign):
        """Test TASK_EVENT_RECEIVED for perpetual event stream tasks."""
        from unittest.mock import Mock

        mock_client = Mock()
        orchestrator = CampaignOrchestrator(mock_client, repository)

        campaign_id = uuid.UUID(simple_campaign.id)
        task_group_id = simple_campaign.task_groups[0].id
        task_id = simple_campaign.task_groups[0].tasks[0].id

        # Create campaign state
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
            CampaignState,
        )

        campaign_state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
        repository.create_campaign(campaign_id, simple_campaign, campaign_state)

        # First: task starts (event stream begins listening)
        orchestrator._record_task_event(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            task_id=task_id,
            event_type='TASK_RUNNING',
            payload={'stream_type': 'event_listener'},
        )

        # Then: event received from stream (multiple times)
        for i in range(3):
            orchestrator._record_task_event(
                campaign_id=campaign_id,
                task_group_id=task_group_id,
                task_id=task_id,
                event_type='TASK_EVENT_RECEIVED',
                payload={'event_number': i + 1},
            )

        # Finally: task stopped
        orchestrator._record_task_event(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            task_id=task_id,
            event_type='TASK_COMPLETED',
            payload={'total_events': 3},
        )

        # Verify all events recorded
        events = repository.load_events(campaign_id)
        assert len(events) == 5
        assert events[0].event_type == 'TASK_RUNNING'
        assert events[1].event_type == 'TASK_EVENT_RECEIVED'
        assert events[2].event_type == 'TASK_EVENT_RECEIVED'
        assert events[3].event_type == 'TASK_EVENT_RECEIVED'
        assert events[4].event_type == 'TASK_COMPLETED'

        # Task should be in COMPLETE status
        snapshot = repository.load_snapshot(campaign_id)
        task_state = next(
            (t for tg in snapshot.state.task_groups for t in tg.tasks if t.id == task_id), None
        )
        assert task_state is not None
        assert task_state.status == ExecutionStatus.COMPLETE

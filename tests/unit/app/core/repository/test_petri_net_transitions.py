from __future__ import annotations

import uuid

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
from intersect_orchestrator.app.converters.campaign_to_petri_net import (
    CampaignPetriNetConverter,
)
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator
from intersect_orchestrator.app.core.repository import InMemoryCampaignRepository


class _FakeClient:
    def __init__(self) -> None:
        self.broadcasts: list[bytes] = []
        self.control_plane_manager = None

    def broadcast_message(self, message: bytes) -> None:
        self.broadcasts.append(message)


@pytest.fixture
def repository() -> InMemoryCampaignRepository:
    return InMemoryCampaignRepository()


@pytest.fixture
def simple_campaign() -> Campaign:
    task = Task(
        id=uuid.uuid4(),
        hierarchy='org.fac.system.subsystem.service',
        capability='measure',
        operation_id='op_measure',
        event_name=None,
        task_dependencies=[],
    )
    task_group = TaskGroup(
        id=uuid.uuid4(),
        group_dependencies=[],
        tasks=[task],
        objectives=[],
    )
    return Campaign(
        id=uuid.uuid4(),
        name='test-campaign',
        user='test-user',
        description='Test campaign for petri transitions',
        task_groups=[task_group],
    )


def test_fire_petri_transition_updates_state_and_records_events(
    repository: InMemoryCampaignRepository,
    simple_campaign: Campaign,
) -> None:
    client = _FakeClient()
    orchestrator = CampaignOrchestrator(client, repository)

    campaign_id = simple_campaign.id
    campaign_state = CampaignState.from_campaign(simple_campaign, status=ExecutionStatus.QUEUED)
    repository.create_campaign(campaign_id, simple_campaign, campaign_state)

    petri_net = CampaignPetriNetConverter().convert(simple_campaign)
    orchestrator._campaign_petri_nets[campaign_id] = petri_net

    tg_id_str = str(simple_campaign.task_groups[0].id)
    task_id_str = str(simple_campaign.task_groups[0].tasks[0].id)

    orchestrator.fire_petri_transition(campaign_id, f'activate_{tg_id_str}')
    events = repository.load_events(campaign_id)
    assert events[-1].event_type == 'TASK_GROUP_STARTED'

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot.state.task_groups[0].status == ExecutionStatus.RUNNING

    orchestrator.fire_petri_transition(campaign_id, f'task_{tg_id_str}_{task_id_str}')
    events = repository.load_events(campaign_id)
    assert events[-1].event_type == 'TASK_COMPLETED'

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot.state.task_groups[0].tasks[0].status == ExecutionStatus.COMPLETE

    orchestrator.fire_petri_transition(campaign_id, f'complete_{tg_id_str}')
    events = repository.load_events(campaign_id)
    assert events[-1].event_type == 'TASK_GROUP_COMPLETED'

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot.state.task_groups[0].status == ExecutionStatus.COMPLETE

    orchestrator.fire_petri_transition(campaign_id, 'finalize_campaign')
    events = repository.load_events(campaign_id)
    assert events[-1].event_type == 'CAMPAIGN_COMPLETED'

    snapshot = repository.load_snapshot(campaign_id)
    assert snapshot.state.status == ExecutionStatus.COMPLETE

from __future__ import annotations

import json
import uuid

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator


class FakeControlPlaneManager:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, str, dict[str, str], bool]] = []

    def publish_message(
        self,
        channel: str,
        payload: bytes,
        content_type: str,
        headers: dict[str, str],
        persist: bool,
    ) -> None:
        self.published.append((channel, payload, content_type, headers, persist))


class FakeClient:
    def __init__(self) -> None:
        self.control_plane_manager = FakeControlPlaneManager()
        self.broadcasts: list[bytes] = []

    def broadcast_message(self, message: bytes) -> None:
        self.broadcasts.append(message)


def _event_types(broadcasts: list[bytes]) -> list[str]:
    return [json.loads(message.decode('utf-8'))['event']['event_type'] for message in broadcasts]


def _make_campaign(campaign_id: uuid.UUID, step_id: uuid.UUID) -> Campaign:
    """Create a test Campaign with a single task."""
    return Campaign(
        id=str(campaign_id),
        name='test-campaign',
        user='test-user',
        description='Test campaign for orchestrator',
        task_groups=[
            {
                'id': 'task-group-1',
                'tasks': [
                    {
                        'id': str(step_id),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'test-capability',
                        'operation_id': 'test-operation',
                        'output': None,
                        'input': None,
                        'task_dependencies': [],
                        'task_objectives': None,
                    }
                ],
                'group_dependencies': [],
                'objectives': [],
            }
        ],
    )


def test_handle_broker_message_completes_step() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    orchestrator.handle_broker_message(
        b'{}',
        'application/json',
        {
            'campaignId': str(campaign_id),
            'nodeId': str(step_id),
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
        },
    )

    assert _event_types(client.broadcasts) == [
        'STEP_START',
        'STEP_COMPLETE',
        'CAMPAIGN_COMPLETE',
    ]


def test_handle_broker_message_emits_error() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    orchestrator.handle_broker_message(
        b'{}',
        'application/json',
        {
            'campaignId': str(campaign_id),
            'nodeId': str(step_id),
            'has_error': 'true',
            'source': 'org.fac.system.subsystem.service',
        },
    )

    assert _event_types(client.broadcasts)[-1] == 'CAMPAIGN_ERROR_FROM_SERVICE'

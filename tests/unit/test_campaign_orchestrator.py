from __future__ import annotations

import json
import uuid

import pytest

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
        self.orchestrator_base_topic = 'test/orchestrator'

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
                'id': str(uuid.uuid4()),
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

    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        {
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
            'campaign_id': str(campaign_id),
            'request_id': str(step_id),
            'message_id': str(uuid.uuid4()),
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
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

    orchestrator.handle_request_reply_broker_message(
        b"I'm an error, here to ruin your program.",
        'text/plain',
        {
            'campaign_id': str(campaign_id),
            'request_id': str(step_id),
            'message_id': str(uuid.uuid4()),
            'has_error': 'true',
            'source': 'org.fac.system.subsystem.service',
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
        },
    )

    assert _event_types(client.broadcasts)[-1] == 'CAMPAIGN_ERROR_FROM_SERVICE'


def test_cancel_campaign() -> None:
    """Test canceling an active campaign."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)
    result = orchestrator.cancel_campaign(campaign_id)

    assert result is True
    assert _event_types(client.broadcasts)[-1] == 'UNKNOWN_ERROR'


def test_cancel_nonexistent_campaign() -> None:
    """Test canceling a campaign that doesn't exist."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    result = orchestrator.cancel_campaign(campaign_id)

    assert result is False


def test_get_campaign() -> None:
    """Test retrieving a campaign from the repository."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)
    retrieved = orchestrator.get_campaign(campaign_id)

    assert retrieved is not None
    assert retrieved.id == campaign_id


def test_get_nonexistent_campaign() -> None:
    """Test retrieving a campaign that doesn't exist."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    retrieved = orchestrator.get_campaign(campaign_id)

    assert retrieved is None


def test_get_campaign_state() -> None:
    """Test retrieving campaign state from the repository."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)
    state = orchestrator.get_campaign_state(campaign_id)

    assert state is not None
    assert len(state.task_groups) > 0


def test_get_campaign_state_nonexistent() -> None:
    """Test retrieving state for a campaign that doesn't exist."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    state = orchestrator.get_campaign_state(campaign_id)

    assert state is None


def test_get_campaign_petri_net() -> None:
    """Test retrieving the Petri Net for a campaign."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)
    petri_net = orchestrator.get_campaign_petri_net(campaign_id)

    assert petri_net is not None


def test_get_campaign_petri_net_nonexistent() -> None:
    """Test retrieving Petri Net for a campaign that doesn't exist."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    petri_net = orchestrator.get_campaign_petri_net(campaign_id)

    assert petri_net is None


def test_fire_petri_transition() -> None:
    """Test firing a Petri Net transition."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)
    petri_net = orchestrator.get_campaign_petri_net(campaign_id)

    # Find an enabled transition
    enabled_transitions = [t.name for t in petri_net.transition() if t.enabled(petri_net)]
    if enabled_transitions:
        orchestrator.fire_petri_transition(campaign_id, enabled_transitions[0])


def test_fire_petri_transition_nonexistent_campaign() -> None:
    """Test firing a transition for a nonexistent campaign."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()

    with pytest.raises(ValueError, match='Petri Net not found'):
        orchestrator.fire_petri_transition(campaign_id, 'some_transition')


def test_fire_petri_transition_nonexistent_transition() -> None:
    """Test firing a transition that doesn't exist."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    with pytest.raises(ValueError, match='does not exist'):
        orchestrator.fire_petri_transition(campaign_id, 'nonexistent_transition')


def test_fire_petri_transition_disabled() -> None:
    """Test firing a transition that is not enabled."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)
    petri_net = orchestrator.get_campaign_petri_net(campaign_id)

    # Find a disabled transition
    disabled_transitions = [t.name for t in petri_net.transition() if not t.enabled(petri_net)]
    if disabled_transitions:
        with pytest.raises(ValueError, match='not enabled'):
            orchestrator.fire_petri_transition(campaign_id, disabled_transitions[0])


def test_submit_duplicate_campaign() -> None:
    """Test submitting a campaign with a duplicate ID."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    with pytest.raises(ValueError, match='already registered'):
        orchestrator.submit_campaign(campaign)


def test_handle_broker_message_invalid_json() -> None:
    """Test handling a broker message with invalid JSON."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    # Invalid JSON should be handled gracefully
    orchestrator.handle_request_reply_broker_message(
        b'not valid json',
        'application/json',
        {
            'campaign_id': str(campaign_id),
            'request_id': str(step_id),
            'message_id': str(uuid.uuid4()),
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
        },
    )


def test_handle_broker_message_no_campaign_id() -> None:
    """Test handling a broker message without a campaign ID."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        # campaign ID is the first thing we check, so might as well not bother including other headers
        {},
    )


def test_handle_broker_message_unknown_campaign() -> None:
    """Test handling a broker message for an unknown campaign."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        {
            'campaign_id': str(uuid.uuid4()),
            'request_id': str(uuid.uuid4()),
            'message_id': str(uuid.uuid4()),
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
        },
    )


def test_handle_broker_message_no_node_id() -> None:
    """Test handling a broker message without a node ID."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        {'campaign_id': str(campaign_id)},
    )


def test_handle_broker_message_wrong_node_id() -> None:
    """Test handling a broker message with wrong node ID."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    # Send message with different node ID
    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        {
            'campaign_id': str(campaign_id),
            'request_id': str(uuid.uuid4()),  # Different from step_id
            'message_id': str(uuid.uuid4()),
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
        },
    )


def test_handle_broker_message_with_error_message() -> None:
    """Test handling a broker message with an error."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    orchestrator.handle_request_reply_broker_message(
        b'{"error": "Something went wrong"}',
        'application/json',
        {
            'campaign_id': str(campaign_id),
            'request_id': str(step_id),
            'has_error': 'true',
            'source': 'org.fac.system.subsystem.service',
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
        },
    )

    events = _event_types(client.broadcasts)
    assert 'CAMPAIGN_ERROR_FROM_SERVICE' in events

from __future__ import annotations

import json
import uuid

import pytest

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator
from intersect_orchestrator.app.core.objective_checkers import IterateChecker
from intersect_orchestrator.app.core.repository import InMemoryCampaignRepository


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
        self.event_subscriptions: list[str] = []

    def broadcast_message(self, message: bytes) -> None:
        self.broadcasts.append(message)

    def subscribe_to_events(
        self, service_hierarchy: str, capability_name: str, event_name: str
    ) -> None:
        channel = f'{service_hierarchy.replace(".", "/")}/events/{capability_name}/{event_name}'
        self.event_subscriptions.append(channel)

    def get_orchestrator_hierarchy(self) -> str:
        return self.orchestrator_base_topic.replace('/', '.')

    def publish_request_message(
        self,
        service_hierarchy: str,
        payload: bytes,
        content_type: str,
        headers: dict[str, str],
        persist: bool = True,
    ) -> None:
        channel = f'{service_hierarchy.replace(".", "/")}/request'
        self.control_plane_manager.publish_message(channel, payload, content_type, headers, persist)


def _event_types(broadcasts: list[bytes]) -> list[str]:
    return [json.loads(message.decode('utf-8'))['event']['event_type'] for message in broadcasts]


def _make_campaign(campaign_id: uuid.UUID, step_id: uuid.UUID) -> Campaign:
    """Create a test Campaign with a single task."""
    return Campaign(
        id=campaign_id,
        run_id=campaign_id,
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


def _make_event_campaign(campaign_id: uuid.UUID, step_id: uuid.UUID) -> Campaign:
    """Create a campaign with a single event-listener task."""
    return Campaign(
        id=campaign_id,
        run_id=campaign_id,
        name='test-event-campaign',
        user='test-user',
        description='Test campaign for event handling',
        task_groups=[
            {
                'id': str(uuid.uuid4()),
                'tasks': [
                    {
                        'id': str(step_id),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'RandomNumberGenerator',
                        'event_name': 'newMeasurement',
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


def test_dispatch_request_uses_task_input_defaults_in_payload() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = Campaign(
        id=campaign_id,
        run_id=campaign_id,
        name='test-campaign',
        user='test-user',
        description='Test campaign for request payload defaults',
        task_groups=[
            {
                'id': str(uuid.uuid4()),
                'tasks': [
                    {
                        'id': str(step_id),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'RandomNumberGenerator',
                        'operation_id': 'generate_random_number',
                        'output': None,
                        'input': {
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'seed': {'type': 'integer', 'default': 7},
                                    'stream_id': {'type': 'string', 'default': 'x'},
                                },
                            },
                            'values': [
                                {'id': str(uuid.uuid4()), 'var': 'seed'},
                                {'id': str(uuid.uuid4()), 'var': 'stream_id'},
                            ],
                        },
                        'task_dependencies': [],
                        'task_objectives': None,
                    }
                ],
                'group_dependencies': [],
                'objectives': [],
            }
        ],
    )

    orchestrator.submit_campaign(campaign)

    assert len(client.control_plane_manager.published) == 1
    published = client.control_plane_manager.published[0]
    published_channel = published[0]
    published_payload = published[1]
    published_headers = published[3]
    assert published_channel == 'org/fac/system/subsystem/service/request'
    assert published_headers['source'] == 'test.orchestrator'
    assert json.loads(published_payload.decode('utf-8')) == {
        'seed': 7,
        'stream_id': 'x',
    }


def test_dispatch_event_subscribes_to_service_events() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_event_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    assert client.event_subscriptions == [
        'org/fac/system/subsystem/service/events/RandomNumberGenerator/newMeasurement'
    ]


def test_handle_event_broker_message_completes_event_task() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_event_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    orchestrator.handle_event_broker_message(
        b'{"value": 42}',
        'application/json',
        {
            'message_id': str(uuid.uuid4()),
            'source': 'org.fac.system.subsystem.service',
            'created_at': '2024-01-01T00:00:00Z',
            'sdk_version': '0.0.1',
            'data_handler': 'MESSAGE',
            'capability_name': 'RandomNumberGenerator',
            'event_name': 'newMeasurement',
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
    repository = InMemoryCampaignRepository()
    orchestrator = CampaignOrchestrator(client, repository=repository)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    wrong_step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    # Send message with different node ID
    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        {
            'campaign_id': str(campaign_id),
            'request_id': str(wrong_step_id),  # Different from step_id
            'message_id': str(uuid.uuid4()),
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
            'destination': 'test.orchestrator',
            'sdk_version': '0.0.1',
            'created_at': '2024-01-01T00:00:00Z',
            'operation_id': 'test-capability.test-operation',
        },
    )

    broadcast = json.loads(client.broadcasts[-1].decode('utf-8'))
    assert broadcast['event']['event_type'] == 'CAMPAIGN_ERROR_FROM_SERVICE'
    assert broadcast['event']['step_id'] == str(wrong_step_id)

    events = list(repository.load_events(campaign_id))
    assert events[-1].event_type == 'CAMPAIGN_ERROR'
    assert events[-1].payload['step_id'] == str(wrong_step_id)


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


# ---------------------------------------------------------------------------
# Objective checker and iterative execution tests
# ---------------------------------------------------------------------------


def _make_iterative_campaign(
    campaign_id: uuid.UUID,
    task_group_id: uuid.UUID,
    step_ids: list[uuid.UUID],
    iterations: int,
) -> Campaign:
    """Create a campaign with parallel tasks and an ObjectiveIterate objective."""
    return Campaign(
        id=campaign_id,
        run_id=campaign_id,
        name='iterative-campaign',
        user='test-user',
        description='Iterative campaign for orchestrator tests',
        task_groups=[
            {
                'id': str(task_group_id),
                'tasks': [
                    {
                        'id': str(sid),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'test-capability',
                        'operation_id': 'test-operation',
                        'output': None,
                        'input': None,
                        'task_dependencies': [],
                        'task_objectives': None,
                    }
                    for sid in step_ids
                ],
                'group_dependencies': [],
                'objectives': [
                    {
                        'id': str(uuid.uuid4()),
                        'type': 'iterate',
                        'iterations': iterations,
                    }
                ],
            }
        ],
    )


def _reply_headers(campaign_id: uuid.UUID, step_id: uuid.UUID) -> dict[str, str]:
    return {
        'has_error': 'false',
        'source': 'org.fac.system.subsystem.service',
        'campaign_id': str(campaign_id),
        'request_id': str(step_id),
        'message_id': str(uuid.uuid4()),
        'destination': 'test.orchestrator',
        'sdk_version': '0.0.1',
        'created_at': '2024-01-01T00:00:00Z',
        'operation_id': 'test-capability.test-operation',
    }


def test_build_task_group_executions_with_iterate() -> None:
    """_build_task_group_executions should create IterateChecker from ObjectiveIterate."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    tg_id = uuid.uuid4()
    step_ids = [uuid.uuid4(), uuid.uuid4()]
    campaign = _make_iterative_campaign(campaign_id, tg_id, step_ids, iterations=5)

    executions = orchestrator._build_task_group_executions(campaign)

    assert len(executions) == 1
    ex = executions[0]
    assert ex.task_group_id == tg_id
    assert len(ex.task_ids) == 2
    assert len(ex.objective_checkers) == 1
    assert isinstance(ex.objective_checkers[0], IterateChecker)
    assert not ex.objectives_met()


def test_no_objectives_runs_once() -> None:
    """A task group with no objectives should execute exactly once."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    campaign = _make_campaign(campaign_id, step_id)

    orchestrator.submit_campaign(campaign)

    # Reply for the single task
    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        _reply_headers(campaign_id, step_id),
    )

    events = _event_types(client.broadcasts)
    assert events == ['STEP_START', 'STEP_COMPLETE', 'CAMPAIGN_COMPLETE']


def test_iterative_campaign_two_iterations() -> None:
    """Two parallel tasks with 2 iterations should produce 4 STEP_COMPLETE events."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    tg_id = uuid.uuid4()
    step_a = uuid.uuid4()
    step_b = uuid.uuid4()
    campaign = _make_iterative_campaign(campaign_id, tg_id, [step_a, step_b], iterations=2)

    orchestrator.submit_campaign(campaign)

    for _ in range(2):
        # Both tasks dispatch in parallel; complete them in order
        orchestrator.handle_request_reply_broker_message(
            b'{}',
            'application/json',
            _reply_headers(campaign_id, step_a),
        )
        orchestrator.handle_request_reply_broker_message(
            b'{}',
            'application/json',
            _reply_headers(campaign_id, step_b),
        )

    events = _event_types(client.broadcasts)

    step_starts = [e for e in events if e == 'STEP_START']
    step_completes = [e for e in events if e == 'STEP_COMPLETE']
    assert len(step_starts) == 4  # 2 tasks x 2 iterations
    assert len(step_completes) == 4
    assert events[-1] == 'CAMPAIGN_COMPLETE'


# ---------------------------------------------------------------------------
# Task dependency sequencing and output-to-input wiring tests
# ---------------------------------------------------------------------------


def _started_step_ids(broadcasts: list[bytes]) -> list[str]:
    """Return step_id strings for all STEP_START events, in order."""
    return [
        json.loads(m.decode())['event']['step_id']
        for m in broadcasts
        if json.loads(m.decode())['event']['event_type'] == 'STEP_START'
    ]


def test_task_dependencies_gate_dispatch() -> None:
    """Task B (depends on A) must NOT start until task A completes."""
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    task_a = uuid.uuid4()
    task_b = uuid.uuid4()

    campaign = Campaign(
        id=campaign_id,
        run_id=campaign_id,
        name='sequential-tasks',
        user='test-user',
        description='task B depends on task A',
        task_groups=[
            {
                'id': str(uuid.uuid4()),
                'tasks': [
                    {
                        'id': str(task_a),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'cap',
                        'operation_id': 'op-a',
                        'output': None,
                        'input': None,
                        'task_dependencies': [],
                        'task_objectives': None,
                    },
                    {
                        'id': str(task_b),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'cap',
                        'operation_id': 'op-b',
                        'output': None,
                        'input': None,
                        'task_dependencies': [str(task_a)],
                        'task_objectives': None,
                    },
                ],
                'group_dependencies': [],
                'objectives': [],
            }
        ],
    )

    orchestrator.submit_campaign(campaign)

    # After submit only task A should have received STEP_START
    started = _started_step_ids(client.broadcasts)
    assert started == [str(task_a)], (
        f'Expected only task A to start initially, got step_ids: {started}'
    )

    # Complete task A
    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        _reply_headers(campaign_id, task_a),
    )

    # Now task B should also have started
    started_after_a = _started_step_ids(client.broadcasts)
    assert str(task_b) in started_after_a, (
        f'Task B should start after task A completes; started IDs: {started_after_a}'
    )

    # Complete task B → campaign finishes
    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        _reply_headers(campaign_id, task_b),
    )

    assert _event_types(client.broadcasts)[-1] == 'CAMPAIGN_COMPLETE'


def test_output_value_wired_to_dependent_task_input() -> None:
    """Output value from task A (resolved at completion) should override the
    default in task B's input when the same value-ID is referenced.
    """
    shared_value_id = uuid.uuid4()

    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    task_a = uuid.uuid4()
    task_b = uuid.uuid4()

    campaign = Campaign(
        id=campaign_id,
        run_id=campaign_id,
        name='output-wiring',
        user='test-user',
        description='task A output wired to task B input via shared value ID',
        task_groups=[
            {
                'id': str(uuid.uuid4()),
                'tasks': [
                    {
                        'id': str(task_a),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'cap',
                        'operation_id': 'initialize-op',
                        'output': {
                            'schema': {
                                'type': 'object',
                                'properties': {'workflow_id': {'type': 'string'}},
                            },
                            'values': [{'id': str(shared_value_id), 'var': 'workflow_id'}],
                        },
                        'input': None,
                        'task_dependencies': [],
                        'task_objectives': None,
                    },
                    {
                        'id': str(task_b),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'cap',
                        'operation_id': 'update-op',
                        'output': None,
                        'input': {
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'workflow_id': {
                                        'type': 'string',
                                        'default': '000000000000000000000001',
                                    }
                                },
                            },
                            'values': [{'id': str(shared_value_id), 'var': 'workflow_id'}],
                        },
                        'task_dependencies': [str(task_a)],
                        'task_objectives': None,
                    },
                ],
                'group_dependencies': [],
                'objectives': [],
            }
        ],
    )

    orchestrator.submit_campaign(campaign)

    # Only task A dispatched so far (task B waits on task A)
    assert len(client.control_plane_manager.published) == 1

    # Complete task A returning the real workflow_id as a JSON string
    real_workflow_id = 'abc123def456abc123def456'
    orchestrator.handle_request_reply_broker_message(
        json.dumps(real_workflow_id).encode(),
        'application/json',
        _reply_headers(campaign_id, task_a),
    )

    # Task B should now have been dispatched with the real workflow_id
    assert len(client.control_plane_manager.published) == 2
    task_b_payload = json.loads(client.control_plane_manager.published[1][1].decode())
    assert task_b_payload['workflow_id'] == real_workflow_id, (
        f"Expected workflow_id='{real_workflow_id}' but got: {task_b_payload}"
    )


def test_cross_group_output_resolves_to_downstream_group_input() -> None:
    """Output from a task in Group 1 should flow into a task input in Group 2
    when the same value-ID is referenced.
    """
    shared_value_id = uuid.uuid4()

    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    task_init = uuid.uuid4()
    task_update = uuid.uuid4()
    tg1_id = uuid.uuid4()
    tg2_id = uuid.uuid4()

    campaign = Campaign(
        id=campaign_id,
        run_id=campaign_id,
        name='cross-group-wiring',
        user='test-user',
        description='initialize_workflow in Group 1, update_workflow in Group 2',
        task_groups=[
            {
                'id': str(tg1_id),
                'tasks': [
                    {
                        'id': str(task_init),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'cap',
                        'operation_id': 'initialize-workflow',
                        'output': {
                            'schema': {
                                'type': 'object',
                                'properties': {'workflow_id': {'type': 'string'}},
                            },
                            'values': [{'id': str(shared_value_id), 'var': 'workflow_id'}],
                        },
                        'input': None,
                        'task_dependencies': [],
                        'task_objectives': None,
                    }
                ],
                'group_dependencies': [],
                'objectives': [],
            },
            {
                'id': str(tg2_id),
                'tasks': [
                    {
                        'id': str(task_update),
                        'hierarchy': 'org.fac.system.subsystem.service',
                        'capability': 'cap',
                        'operation_id': 'update-workflow',
                        'output': None,
                        'input': {
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'workflow_id': {
                                        'type': 'string',
                                        'default': '000000000000000000000001',
                                    }
                                },
                            },
                            'values': [{'id': str(shared_value_id), 'var': 'workflow_id'}],
                        },
                        'task_dependencies': [],
                        'task_objectives': None,
                    }
                ],
                'group_dependencies': [str(tg1_id)],
                'objectives': [],
            },
        ],
    )

    orchestrator.submit_campaign(campaign)

    # Group 1: complete task_init with real workflow_id
    real_workflow_id = 'deadbeefdeadbeefdeadbeef'
    orchestrator.handle_request_reply_broker_message(
        json.dumps(real_workflow_id).encode(),
        'application/json',
        _reply_headers(campaign_id, task_init),
    )

    # Group 2 task_update should have been dispatched with the real workflow_id
    assert len(client.control_plane_manager.published) == 2
    task_update_payload = json.loads(client.control_plane_manager.published[1][1].decode())
    assert task_update_payload['workflow_id'] == real_workflow_id, (
        f"Expected workflow_id='{real_workflow_id}' but got: {task_update_payload}"
    )

    # Complete task_update → campaign finishes
    orchestrator.handle_request_reply_broker_message(
        b'{}',
        'application/json',
        _reply_headers(campaign_id, task_update),
    )
    assert _event_types(client.broadcasts)[-1] == 'CAMPAIGN_COMPLETE'

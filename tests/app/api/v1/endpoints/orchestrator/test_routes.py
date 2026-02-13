"""Tests for orchestrator API routes."""

import uuid

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
    ExecutionStatus,
)


def test_start_campaign_success(client, valid_api_key, sample_campaign_data):
    """Test successful campaign start with valid API key."""
    response = client.post(
        '/v1/orchestrator/start_campaign',
        json=sample_campaign_data,
        headers={'Authorization': valid_api_key},
    )

    assert response.status_code == 200
    # Response should be a UUID string
    import uuid

    uuid.UUID(response.json())  # This will raise if not a valid UUID


def test_start_campaign_stores_campaign_state_and_petri_net(
    client,
    valid_api_key,
    campaign_payloads,
):
    """Test campaign start stores payload, state, and Petri Net in memory."""
    orchestrator = client.app.state.campaign_orchestrator

    for payload in campaign_payloads:
        response = client.post(
            '/v1/orchestrator/start_campaign',
            json=payload,
            headers={'Authorization': valid_api_key},
        )

        assert response.status_code == 200

        campaign_uuid = uuid.UUID(response.json())

        stored_campaign = orchestrator.get_campaign(campaign_uuid)
        assert stored_campaign is not None
        assert stored_campaign.id == payload['id']

        stored_state = orchestrator.get_campaign_state(campaign_uuid)
        assert stored_state is not None
        assert stored_state.status == ExecutionStatus.QUEUED
        assert all(
            task_group.status == ExecutionStatus.QUEUED
            for task_group in stored_state.task_groups
        )
        assert all(
            task.status == ExecutionStatus.QUEUED
            for task_group in stored_state.task_groups
            for task in task_group.tasks
        )

        petri_net = orchestrator.get_campaign_petri_net(campaign_uuid)
        assert petri_net is not None
        assert petri_net.name == f"Campaign_{payload['id']}"


def test_start_campaign_invalid_api_key(client, invalid_api_key, sample_campaign_data):
    """Test campaign start with invalid API key."""
    response = client.post(
        '/v1/orchestrator/start_campaign',
        json=sample_campaign_data,
        headers={'Authorization': invalid_api_key},
    )

    assert response.status_code == 401
    assert 'invalid or incorrect API key provided' in response.json()['detail']


def test_start_campaign_missing_api_key(client, sample_campaign_data):
    """Test campaign start without API key."""
    response = client.post('/v1/orchestrator/start_campaign', json=sample_campaign_data)

    assert response.status_code == 403  # FastAPI's default for missing security dependency


def test_start_campaign_invalid_campaign_data(client, valid_api_key):
    """Test campaign start with invalid campaign data."""
    invalid_campaign_data = {'invalid': 'data'}

    response = client.post(
        '/v1/orchestrator/start_campaign',
        json=invalid_campaign_data,
        headers={'Authorization': valid_api_key},
    )

    # Should fail validation due to missing required fields
    assert response.status_code == 422  # Validation error


def test_stop_campaign_success(client, valid_api_key):
    """Test successful campaign stop with valid API key."""
    campaign_uuid = str(uuid.uuid4())

    response = client.post(
        '/v1/orchestrator/stop_campaign',
        json=campaign_uuid,
        headers={'Authorization': valid_api_key},
    )

    # Campaign doesn't exist, so expect 404
    assert response.status_code == 404
    assert 'campaign not found' in response.json()['detail']


def test_stop_campaign_invalid_api_key(client, invalid_api_key):
    """Test campaign stop with invalid API key."""
    campaign_uuid = str(uuid.uuid4())

    response = client.post(
        '/v1/orchestrator/stop_campaign',
        json=campaign_uuid,
        headers={'Authorization': invalid_api_key},
    )

    assert response.status_code == 401
    assert 'invalid or incorrect API key provided' in response.json()['detail']


def test_stop_campaign_missing_api_key(client):
    """Test campaign stop without API key."""
    campaign_uuid = str(uuid.uuid4())

    response = client.post('/v1/orchestrator/stop_campaign', json=campaign_uuid)

    assert response.status_code == 403  # FastAPI's default for missing security dependency


def test_stop_campaign_invalid_uuid(client, valid_api_key):
    """Test campaign stop with invalid UUID format."""
    invalid_uuid = 'not-a-uuid'

    response = client.post(
        '/v1/orchestrator/stop_campaign',
        json=invalid_uuid,
        headers={'Authorization': valid_api_key},
    )

    # Should fail validation due to UUID format
    assert response.status_code == 422  # Validation error

"""Full end-to-end test using the orchestrator REST API and WebSocket.

This test requires:
- RabbitMQ broker running
- Campaign orchestrator running at http://localhost:8000
- Random number service running and connected to broker

Start all services with: docker-compose up
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import socket
from typing import Any

import httpx
import pytest
import websockets

TEST_DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
CAMPAIGN_FILE = TEST_DATA_DIR / 'campaign' / 'random-number-campaign.campaign.json'


def is_orchestrator_available() -> bool:
    """Check if the orchestrator is available."""
    orchestrator_host = os.getenv('ORCHESTRATOR_HOST', 'localhost')
    orchestrator_port = int(os.getenv('ORCHESTRATOR_PORT', '8000'))

    try:
        with socket.create_connection((orchestrator_host, orchestrator_port), timeout=2):
            return True
    except (TimeoutError, ConnectionRefusedError, OSError):
        return False


@pytest.fixture(scope='session', autouse=True)
def check_orchestrator_available() -> None:
    """Check orchestrator availability and skip tests if unavailable."""
    if not is_orchestrator_available():
        pytest.skip(
            f'Campaign orchestrator not available at '
            f'{os.getenv("ORCHESTRATOR_HOST", "localhost")}:{os.getenv("ORCHESTRATOR_PORT", "8000")}. '
            f"Run 'docker-compose up' to start all services."
        )


def load_campaign_json() -> dict[str, Any]:
    """Load campaign JSON from test data."""
    with CAMPAIGN_FILE.open() as f:
        return json.load(f)


def get_orchestrator_url() -> str:
    """Get the base URL for the orchestrator."""
    host = os.getenv('ORCHESTRATOR_HOST', 'localhost')
    port = os.getenv('ORCHESTRATOR_PORT', '8000')
    return f'http://{host}:{port}'


def get_orchestrator_ws_url() -> str:
    """Get the WebSocket URL for the orchestrator."""
    host = os.getenv('ORCHESTRATOR_HOST', 'localhost')
    port = os.getenv('ORCHESTRATOR_PORT', '8000')
    return f'ws://{host}:{port}'


def get_api_key() -> str:
    """Get the API key for authentication."""
    return os.getenv('API_KEY', 'test-api-key-12345678901234567890')


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_campaign_loop_with_websocket() -> None:
    """Test submitting a campaign via REST API and monitoring via WebSocket.

    This test:
    1. Connects to the WebSocket /events endpoint
    2. POSTs a campaign to /start_campaign
    3. Listens for campaign events on the WebSocket
    4. Verifies the campaign completes successfully
    """
    base_url = get_orchestrator_url()
    ws_url = get_orchestrator_ws_url()
    api_key = get_api_key()
    campaign_data = load_campaign_json()

    # Track received events
    received_events: list[dict[str, Any]] = []
    campaign_complete = False

    async def listen_to_events(websocket: websockets.WebSocketClientProtocol) -> None:
        """Listen to WebSocket events and track them."""
        nonlocal campaign_complete
        try:
            async for message in websocket:
                orchestrator_event = json.loads(message)
                received_events.append(orchestrator_event)

                # Extract the nested event
                event = orchestrator_event.get('event', {})
                event_type = event.get('event_type')

                # Check if campaign is complete
                if event_type == 'CAMPAIGN_COMPLETE':
                    campaign_complete = True
                    break

                # Also break on errors
                if event_type in [
                    'CAMPAIGN_ERROR_FROM_SERVICE',
                    'CAMPAIGN_ERROR_SCHEMA',
                ]:
                    break
        except websockets.exceptions.ConnectionClosed:
            pass

    # Connect to WebSocket first
    async with websockets.connect(f'{ws_url}/v1/orchestrator/events') as websocket:
        # Start listening task
        listen_task = asyncio.create_task(listen_to_events(websocket))

        # Give WebSocket a moment to be ready
        await asyncio.sleep(0.5)

        # Submit the campaign via REST API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f'{base_url}/v1/orchestrator/start_campaign',
                json=campaign_data,
                headers={'Authorization': api_key},
                timeout=30.0,
            )
            assert response.status_code == 200, f'Failed to start campaign: {response.text}'

        # Wait for events (with timeout)
        try:
            await asyncio.wait_for(listen_task, timeout=60.0)
        except TimeoutError:
            pytest.fail('Campaign did not complete within 60 seconds')

    # Verify we received events
    assert len(received_events) > 0, 'No events received from WebSocket'

    # Verify campaign completed successfully
    assert campaign_complete, f'Campaign did not complete. Last events: {received_events[-3:]}'

    # Verify we got some expected event types
    event_types = {event.get('event_type') for event in received_events}

    # We should at least see CAMPAIGN_COMPLETE
    assert 'CAMPAIGN_COMPLETE' in event_types, (
        f'Missing CAMPAIGN_COMPLETE event. Got: {event_types}'
    )

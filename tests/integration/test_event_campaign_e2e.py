"""End-to-end test for event-task campaign execution over REST + WebSocket."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.conftest import (
    get_api_key,
    get_orchestrator_url,
    get_orchestrator_ws_url,
)
from tests.integration.campaign_payload_utils import campaign_with_fresh_ids


@pytest.mark.integration
@pytest.mark.asyncio
async def test_event_campaign_completes_via_websocket(
    check_orchestrator_available: None,
    check_random_number_service_available: None,
    event_campaign_json: dict[str, Any],
) -> None:
    """Submit request+event campaign and verify campaign completion events."""
    base_url = get_orchestrator_url()
    ws_url = get_orchestrator_ws_url()
    api_key = get_api_key()
    campaign_data = campaign_with_fresh_ids(event_campaign_json)

    received_events: list[dict[str, Any]] = []
    campaign_complete = False

    async def listen_to_events(websocket: websockets.WebSocketClientProtocol) -> None:
        nonlocal campaign_complete
        try:
            async for message in websocket:
                orchestrator_event = json.loads(message)
                received_events.append(orchestrator_event)

                event = orchestrator_event.get('event', {})
                event_type = event.get('event_type')

                if event_type == 'CAMPAIGN_COMPLETE':
                    campaign_complete = True
                    break

                if event_type in ['CAMPAIGN_ERROR_FROM_SERVICE', 'CAMPAIGN_ERROR_SCHEMA']:
                    break
        except websockets.exceptions.ConnectionClosed:
            pass

    async with websockets.connect(f'{ws_url}/v1/orchestrator/events') as websocket:
        listen_task = asyncio.create_task(listen_to_events(websocket))

        await asyncio.sleep(0.5)

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f'{base_url}/v1/orchestrator/start_campaign',
                json=campaign_data,
                headers={'Authorization': api_key},
                timeout=30.0,
            )
            assert response.status_code == 200, f'Failed to start campaign: {response.text}'

        try:
            await asyncio.wait_for(listen_task, timeout=60.0)
        except TimeoutError:
            pytest.fail('Event campaign did not complete within 60 seconds')

    assert received_events, 'No events received from WebSocket'

    event_types = [event.get('event', {}).get('event_type') for event in received_events]
    step_complete_count = sum(1 for event_type in event_types if event_type == 'STEP_COMPLETE')

    assert campaign_complete, f'Campaign did not complete. Last events: {received_events[-3:]}'
    assert 'CAMPAIGN_COMPLETE' in event_types, f'Missing CAMPAIGN_COMPLETE in events: {event_types}'
    assert step_complete_count >= 2, (
        f'Expected at least 2 STEP_COMPLETE events (request + event task). Got: {step_complete_count}'
    )

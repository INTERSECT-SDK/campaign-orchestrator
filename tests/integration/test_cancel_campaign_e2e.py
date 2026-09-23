"""End-to-end test for cancelling a running campaign over REST + WebSocket.

This test requires:
- RabbitMQ broker running
- Campaign orchestrator running at http://localhost:8000
- Long-running-tasks-service running and connected to broker

The campaign's single task runs for one minute, so the campaign is guaranteed to
still be in flight when the cancellation request lands.

NOTE: cancellation does not notify the service, so it keeps working on the cancelled
task for the rest of its minute. The service handles one request at a time, so any
cancellation test started within that minute (including the second test in this file)
still passes, but its task sits queued at the broker rather than running when it is
cancelled. The orchestrator cannot tell the difference: in both cases the task has been
dispatched and no reply has arrived.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import websockets

from tests.integration.conftest import (
    get_api_key,
    get_orchestrator_url,
    get_orchestrator_ws_url,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

# How long to keep listening after cancellation to make sure the campaign does not progress
POST_CANCEL_QUIET_SECONDS = 5.0


async def _cancel_via_cancellation(client: httpx.AsyncClient, run_id: str) -> httpx.Response:
    return await client.post(f'/campaigns/{run_id}/cancellation', timeout=30.0)


async def _cancel_via_stop_campaign(client: httpx.AsyncClient, run_id: str) -> httpx.Response:
    return await client.post('/stop_campaign', json=run_id, timeout=30.0)


async def _run_cancel_running_campaign(
    campaign_json: dict[str, Any],
    cancel_request: Callable[[httpx.AsyncClient, str], Awaitable[httpx.Response]],
) -> None:
    """Start a long-running campaign, cancel it with ``cancel_request``, and verify it stops."""
    base_url = get_orchestrator_url()
    ws_url = get_orchestrator_ws_url()
    headers = {'Authorization': get_api_key()}
    campaign_data = json.loads(json.dumps(campaign_json))
    run_id = str(uuid.uuid4())
    campaign_data['run_id'] = run_id

    events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def listen_to_events(websocket: websockets.WebSocketClientProtocol) -> None:
        """Queue the nested events that belong to this campaign run."""
        with contextlib.suppress(websockets.exceptions.ConnectionClosed):
            async for message in websocket:
                orchestrator_event = json.loads(message)
                if orchestrator_event.get('run_id') == run_id:
                    await events.put(orchestrator_event.get('event', {}))

    async def wait_for_event(event_type: str, seconds: float) -> dict[str, Any]:
        """Return the next event of ``event_type``, failing on campaign errors or timeout."""
        seen: list[str] = []
        try:
            async with asyncio.timeout(seconds):
                while True:
                    event = await events.get()
                    seen.append(event.get('event_type'))
                    if event.get('event_type') == event_type:
                        return event
                    if event.get('event_type', '').startswith('CAMPAIGN_ERROR'):
                        pytest.fail(f'Campaign errored while waiting for {event_type}: {event}')
        except TimeoutError:
            pytest.fail(f'No {event_type} event within {seconds}s. Saw: {seen}')

    async with (
        websockets.connect(f'{ws_url}/v1/orchestrator/events') as websocket,
        httpx.AsyncClient(base_url=f'{base_url}/v1/orchestrator', headers=headers) as client,
    ):
        listen_task = asyncio.create_task(listen_to_events(websocket))

        # Give WebSocket a moment to be ready
        await asyncio.sleep(0.5)

        try:
            response = await client.post(f'/campaigns/{run_id}', json=campaign_data, timeout=30.0)
            assert response.status_code == 201, f'Failed to start campaign: {response.text}'

            # The task has been dispatched and will keep running for a minute
            await wait_for_event('STEP_START', seconds=30.0)

            response = await cancel_request(client, run_id)
            assert response.status_code == 200, f'Failed to cancel campaign: {response.text}'
            assert response.json() == run_id

            cancel_event = await wait_for_event('UNKNOWN_ERROR', seconds=10.0)
            assert cancel_event['exception_message'] == 'Campaign cancelled by user'

            # The campaign is no longer tracked as running
            response = await client.get('/campaigns', timeout=30.0)
            assert response.status_code == 200
            assert run_id not in {c['campaign_run_id'] for c in response.json()['campaigns']}

            # A second cancellation has nothing to cancel
            response = await cancel_request(client, run_id)
            assert response.status_code == 404

            # The campaign must not keep progressing after cancellation
            await asyncio.sleep(POST_CANCEL_QUIET_SECONDS)
            late_events = [events.get_nowait() for _ in range(events.qsize())]
            assert not late_events, f'Unexpected events after cancellation: {late_events}'
        finally:
            listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listen_task


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cancel_running_campaign(
    check_orchestrator_available: None,
    check_long_running_tasks_service_available: None,
    long_running_task_campaign_json: dict[str, Any],
) -> None:
    """Cancel a running campaign using POST /campaigns/{run_id}/cancellation."""
    await _run_cancel_running_campaign(long_running_task_campaign_json, _cancel_via_cancellation)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cancel_running_campaign_deprecated(
    check_orchestrator_available: None,
    check_long_running_tasks_service_available: None,
    long_running_task_campaign_json: dict[str, Any],
) -> None:
    """Cancel a running campaign using the deprecated POST /stop_campaign."""
    await _run_cancel_running_campaign(long_running_task_campaign_json, _cancel_via_stop_campaign)

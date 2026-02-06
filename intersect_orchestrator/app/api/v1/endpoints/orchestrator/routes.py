"""These are the 'real' endpoints called by the SDK in a production environment."""

import asyncio
import json
from typing import TYPE_CHECKING, Annotated

from fastapi import (
    APIRouter,
    Body,
    HTTPException,
    Request,
    Security,
    WebSocket,
    WebSocketDisconnect,
)

from .....core.environment import settings
from .....core.log_config import logger
from ...api_key import api_key_header
from .models.icmp import Icmp, IntersectCampaignId
from .models.orchestrator_events import OrchestratorEvent

if TYPE_CHECKING:
    from .....core.intersect_client import CoreServiceIntersectClient

router = APIRouter()


@router.post(
    '/start_campaign',
    description='Initialize campaign',
    response_description=('Metadata about the successful START CAMPAIGN information'),
)
async def start_campaign(
    request: Request,
    icmp: Annotated[Icmp, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    # TODO
    return 'TODO'


@router.post(
    '/stop_campaign',
    description='Stop campaign',
    response_description=('Metadata about the successful STOP CAMPAIGN information'),
)
async def stop_campaign(
    request: Request,
    camapign_uuid: Annotated[IntersectCampaignId, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    # NOTE: we only keep track of RUNNING campaigns, stopped campaigns might as well not exist
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    # TODO
    return 'TODO'


@router.websocket(
    '/events',
)
async def campaign_events(websocket: WebSocket):
    """Endpoint to handle emitting events to websocket clients."""
    """
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    """

    await websocket.accept()
    client: CoreServiceIntersectClient = websocket.app.state.intersect_client
    queue = client.add_http_connection()
    try:
        while True:
            msg: bytes = await queue.get()
            queue.task_done()
            if len(msg) == 0:  # indicates force-quit sent from client
                break
            await websocket.send_text(msg.decode('utf-8'))
            # newmsg = json.dumps(msg.decode('utf-8'))
            # await websocket.send_json(newmsg)
    except asyncio.CancelledError:
        client.remove_http_connection(queue)
        await websocket.close()
        raise
    except WebSocketDisconnect:
        client.remove_http_connection(queue)

    await websocket.close()


from fastapi.responses import HTMLResponse  # noqa: E402


@router.get('/test')
async def websocket_ui():
    html = """
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <h2>Your ID: <span id="ws-id"></span></h2>
        <ul id='messages'>
        </ul>
        <script>
            var client_id = Date.now()
            document.querySelector("#ws-id").textContent = client_id;
            var ws = new WebSocket(`ws://localhost:8000/v1/orchestrator/events`);
            // var ws = new WebSocket(`ws://localhost:8000/v1/orchestrator/ws/${client_id}`);
            ws.onmessage = function(event) {
                console.log(event.data);
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                message.appendChild(content)
                messages.appendChild(message)
            };
            ws.onclose = (event) => {console.log('closed', event);}
            ws.onerror = (event) => {console.log('error', event);}
        </script>
    </body>
</html>

"""
    return HTMLResponse(html)

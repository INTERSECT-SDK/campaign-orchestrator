"""These are the 'real' endpoints called by the SDK in a production environment."""

import asyncio
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
from ...api_key import api_key_header

if TYPE_CHECKING:
    from .....core.intersect_client import CoreServiceIntersectClient
from .models.campaign import Campaign, IntersectCampaignId

router = APIRouter()


@router.post(
    '/start_campaign',
    description='Initialize campaign',
    response_description=('Metadata about the successful START CAMPAIGN information'),
)
async def start_campaign(
    request: Request,
    campaign: Annotated[Campaign, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    orchestrator = request.app.state.campaign_orchestrator
    campaign_id = orchestrator.submit_campaign(campaign)
    return str(campaign_id)


@router.post(
    '/stop_campaign',
    description='Stop campaign',
    response_description=('Metadata about the successful STOP CAMPAIGN information'),
)
async def stop_campaign(
    request: Request,
    campaign_uuid: Annotated[IntersectCampaignId, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    # NOTE: we only keep track of RUNNING campaigns, stopped campaigns might as well not exist
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    orchestrator = request.app.state.campaign_orchestrator
    if not orchestrator.cancel_campaign(campaign_uuid):
        raise HTTPException(status_code=404, detail='campaign not found')
    return str(campaign_uuid)


@router.websocket(
    '/events',
)
async def campaign_events(websocket: WebSocket) -> None:
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
async def websocket_ui() -> HTMLResponse:
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

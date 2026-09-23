"""These are the 'real' endpoints called by the SDK in a production environment."""

import asyncio
from typing import TYPE_CHECKING, Annotated

from fastapi import (
    APIRouter,
    Body,
    HTTPException,
    Query,
    Request,
    Security,
    WebSocket,
    WebSocketDisconnect,
)
from snakes import SnakesError

from .....core.environment import settings
from ...api_key import api_key_header

if TYPE_CHECKING:
    from .....core.intersect_client import CoreServiceIntersectClient
from .models.campaign import Campaign, IntersectCampaignId
from .models.campaign_state import CampaignListResponse, ExecutionStatus

router = APIRouter()


@router.get(
    '/campaigns',
    description='List campaigns',
    response_description='List of campaigns with their IDs and status',
)
async def list_campaigns(
    request: Request,
    api_key: Annotated[str, Security(api_key_header)],
    status: Annotated[list[ExecutionStatus] | None, Query()] = None,
) -> CampaignListResponse:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    orchestrator = request.app.state.campaign_orchestrator
    campaigns = orchestrator.list_campaigns(status=status)
    return CampaignListResponse(campaigns=campaigns)


def _submit_campaign(request: Request, campaign: Campaign) -> str:
    orchestrator = request.app.state.campaign_orchestrator
    try:
        campaign_id = orchestrator.submit_campaign(campaign)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except SnakesError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    return str(campaign_id)


def _cancel_campaign(request: Request, campaign_run_id: IntersectCampaignId) -> str:
    # NOTE: we only keep track of RUNNING campaigns, stopped campaigns might as well not exist
    orchestrator = request.app.state.campaign_orchestrator
    if not orchestrator.cancel_campaign(campaign_run_id):
        raise HTTPException(status_code=404, detail='campaign not found')
    return str(campaign_run_id)


@router.post(
    '/campaigns/{run_id}',
    status_code=201,
    description='Submit and start a campaign run. The run_id in the path must match the run_id in the body.',
    response_description='The run ID of the started campaign',
)
async def create_campaign(
    request: Request,
    run_id: IntersectCampaignId,
    campaign: Annotated[Campaign, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    if campaign.run_id != run_id:
        raise HTTPException(
            status_code=400,
            detail=f'run_id in path ({run_id}) does not match run_id in body ({campaign.run_id})',
        )
    return _submit_campaign(request, campaign)


@router.post(
    '/campaigns/{run_id}/cancellation',
    description='Cancel a running campaign',
    response_description='The run ID of the cancelled campaign',
)
async def cancel_campaign(
    request: Request,
    run_id: IntersectCampaignId,
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    return _cancel_campaign(request, run_id)


@router.post(
    '/start_campaign',
    description='Initialize campaign. Deprecated: use POST /campaigns/{run_id} instead.',
    response_description=('Metadata about the successful START CAMPAIGN information'),
    deprecated=True,
)
async def start_campaign(
    request: Request,
    campaign: Annotated[Campaign, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    return _submit_campaign(request, campaign)


@router.post(
    '/stop_campaign',
    description='Stop campaign. Deprecated: use POST /campaigns/{run_id}/cancellation instead.',
    response_description=('Metadata about the successful STOP CAMPAIGN information'),
    deprecated=True,
)
async def stop_campaign(
    request: Request,
    campaign_uuid: Annotated[IntersectCampaignId, Body(media_type='application/json')],
    api_key: Annotated[str, Security(api_key_header)],
) -> str:
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    return _cancel_campaign(request, campaign_uuid)


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
    finally:
        try:
            await websocket.close()
        except RuntimeError:
            # WebSocket already closed
            pass


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

"""These are the 'real' endpoints called by the SDK in a production environment."""

import asyncio
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Request, Security
from sse_starlette.sse import EventSourceResponse

from .....core.environment import settings
from .....core.log_config import logger
from ...api_key import api_key_header
from .models.icmp import Icmp, IntersectCampaignId
from .models.orchestrator_events import OrchestratorEvent

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


@router.get(
    '/events',
    description='SSE endpoint to obtain all events from the orchestrator',
    response_description='event information will vary based on type',
    response_model=OrchestratorEvent,
)
async def campaign_events(
    request: Request,
    api_key: Annotated[str, Security(api_key_header)],
):
    async def event_publisher():
        # TODO this whole function is currently fake
        while True:
            try:
                yield 'hello'
                await asyncio.sleep(1.0)
            except asyncio.CancelledError as e:
                logger.info('client disconnected %s', e)
                raise

    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail='invalid or incorrect API key provided')
    return EventSourceResponse(event_publisher())

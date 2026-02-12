"""Main file to start backend server."""

import typing
from contextlib import asynccontextmanager
from importlib.metadata import version

from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI

from .api import router as api_router
from .core.campaign_orchestrator import CampaignOrchestrator
from .core.environment import settings
from .core.intersect_client import CoreServiceIntersectClient
from .core.log_config import logger, setup_logging
from .middlewares.logging_context import add_logging_middleware

# this needs to be called per uvicorn worker
setup_logging()


@asynccontextmanager
async def lifespan(_app: FastAPI) -> typing.AsyncGenerator[None, None]:  # noqa: ARG001
    # On startup
    logger.info('Initializing app')

    # TODO - add broker connection here later
    app.state.intersect_client = CoreServiceIntersectClient(settings)
    app.state.campaign_orchestrator = CampaignOrchestrator(app.state.intersect_client)
    app.state.intersect_client.set_campaign_orchestrator(app.state.campaign_orchestrator)
    if not app.state.intersect_client.can_reconnect():
        logger.critical('Unable to connect to INTERSECT broker, exiting')
        import sys

        sys.exit(1)

    logger.info('App initialized')

    yield

    # On cleanup
    logger.info('Shutting down gracefully')
    app.state.intersect_client.terminate()
    logger.info('Graceful shutdown complete')


app = FastAPI(
    debug=True,
    title='INTERSECT Orchestrator',
    description='Execute INTERSECT Campaigns',
    version=version('intersect-orchestrator'),
    # only provide API documentation for public API URLs. Do not provide documentation for the UI URLs.
    redoc_url='/',
    docs_url='/docs',
    openapi_url='/openapi.json',
    lifespan=lifespan,
)

# Middlewares are executed in REVERSE order from when they are added

add_logging_middleware(app)
app.add_middleware(CorrelationIdMiddleware)

# routes, only the API route has API documentation
app.include_router(api_router)

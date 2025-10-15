"""Main file to start backend server."""

import typing
from contextlib import asynccontextmanager
from importlib.metadata import version

from asgi_correlation_id import CorrelationIdMiddleware
from fastapi import FastAPI

from .api import router as api_router
from .core.log_config import logger, setup_logging
from .middlewares.logging_context import add_logging_middleware

# this needs to be called per uvicorn worker
setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> typing.AsyncGenerator[None, None]:
    # On startup
    logger.info('Initializing app')

    # TODO - add broker connection here later

    logger.info('App initialized')

    yield

    # On cleanup
    logger.info('Shutting down gracefully')

    logger.info('Graceful shutdown complete')


app = FastAPI(
    debug=True,
    title='INTERSECT Registry Service',
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

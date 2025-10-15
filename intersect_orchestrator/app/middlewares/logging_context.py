"""Module to log information about each request. Credit to https://gist.github.com/nymous/f138c7f06062b7c43c060bf03759c29e"""

import time
from collections.abc import Awaitable, Callable

import structlog
from asgi_correlation_id.context import correlation_id
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse
from uvicorn.protocols.utils import get_path_with_query_string

_access_logger: structlog.stdlib.BoundLogger = structlog.get_logger(
    'intersect-registry-service.access'
)
_error_logger: structlog.stdlib.BoundLogger = structlog.get_logger(
    'intersect-registry-service.error'
)


def add_logging_middleware(app: FastAPI) -> None:
    @app.middleware('http')
    async def logging_middleware(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        structlog.contextvars.clear_contextvars()
        # These context vars will be added to all log entries emitted during the request
        request_id = correlation_id.get()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        start_time = time.perf_counter_ns()
        # If the call_next raises an error, we still want to return our own 500 response,
        # so we can add headers to it (process time, request ID...)
        response: Response = PlainTextResponse('Internal server error', status_code=500)
        try:
            response = await call_next(request)
        except Exception:
            # this will never be an HTTPException, this gets caught elsewhere
            _error_logger.exception('Uncaught exception')
        finally:
            process_time = time.perf_counter_ns() - start_time
            status_code = response.status_code
            url = get_path_with_query_string(request.scope)  # type: ignore[arg-type]
            client_host = request.client.host  # type: ignore[union-attr]
            client_port = request.client.port  # type: ignore[union-attr]
            http_method = request.method
            http_version = request.scope['http_version']
            # Recreate the Uvicorn access log format, but add all parameters as structured information
            # This does NOT log HTTP headers or form data
            _access_logger.info(
                '%s',
                f"""{client_host}:{client_port} - "{http_method} {url} HTTP/{http_version}" {status_code}""",
                http={
                    'url': str(request.url),
                    'status_code': status_code,
                    'method': http_method,
                    'request_id': request_id,
                    'version': http_version,
                },
                network={'client': {'ip': client_host, 'port': client_port}},
                duration=process_time,
            )
            response.headers['X-Process-Time'] = str(process_time / 10**9)
            return response  # noqa: B012 (TODO may not want to silence exceptions here)

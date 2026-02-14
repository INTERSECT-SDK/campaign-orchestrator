from fastapi import APIRouter, Request, Response

router = APIRouter()


@router.get(
    '/ping',
    tags=['Ping'],
    description='Application ping',
    response_description=('empty response, 204 if able to execute and 5xx if not'),
)
async def ping() -> Response:
    """rudimentary ping"""
    return Response(status_code=204)


@router.get(
    '/healthcheck',
    tags=['Healthcheck'],
    description='Application healthcheck',
    response_description=(
        "Array of errors explaining why the service won't work (if empty array: all OK)"
    ),
)
async def healthcheck(request: Request) -> Response:
    """This can be used as a healthcheck endpoint for, e.g. Kubernetes."""
    # check that we're connected to the broker
    return Response()

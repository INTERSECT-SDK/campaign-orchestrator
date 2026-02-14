import structlog
import uvicorn

from intersect_orchestrator.app.core.environment import settings
from intersect_orchestrator.app.core.log_config import setup_logging

logger = structlog.stdlib.get_logger('intersect-orchestrator.main')


def main() -> None:
    # WARNING - the logger names will NOT propogate to workers if uvicorn.reload = True or uvicorn.server_workers > 1
    # so we should setup logging twice - once on the uvicorn main, and once in the runner
    setup_logging()

    host = '0.0.0.0' if settings.PRODUCTION else '127.0.0.1'  # noqa: S104 (mandatory if running in Docker)
    url = f'http://{host}:{settings.SERVER_PORT}{settings.BASE_URL}'
    if settings.PRODUCTION:
        logger.info('Running server at %s', url)
    else:
        reload_str = ', server will reload on file changes' if settings.SERVER_WORKERS == 1 else ''
        logger.info('Running DEVELOPMENT server at %s%s', url, reload_str)
        logger.info('View docs at %s/docs', url)

    uvicorn.run(
        'intersect_orchestrator.app.main:app',
        host=host,
        port=settings.SERVER_PORT,
        reload=(not settings.PRODUCTION and settings.SERVER_WORKERS == 1),
        workers=settings.SERVER_WORKERS,
        root_path=settings.BASE_URL,
        proxy_headers=settings.PRODUCTION,
        forwarded_allow_ips='*'
        if settings.PRODUCTION
        else '127.0.0.1',  # This assumes that you will always
        server_header=False,
        # override Uvicorn's loggers with our own, and disable the uvicorn access logger
        log_config=None,
        access_log=False,
    )


if __name__ == '__main__':
    main()

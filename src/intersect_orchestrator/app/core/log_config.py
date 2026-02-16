"""Much of this module is taken from https://www.structlog.org/en/stable/standard-library.html#rendering-using-structlog-based-formatters-within-logging and https://gist.github.com/nymous/f138c7f06062b7c43c060bf03759c29e"""

import logging
import logging.config
import os
import platform
from typing import Any

import structlog
from structlog.types import EventDict

from .environment import settings

logger: structlog.stdlib.BoundLogger = structlog.get_logger('intersect-orchestrator.app')
"""
Use this logger when wanting to log something specific in the application

We use the following PRIVATE loggers:
  intersect-orchestrator.access - general access logging
  intersect-orchestrator.error - log any uncaught exception
"""

_BUNYAN_LOG_LEVELS = {
    'critical': 60,
    'error': 50,
    'warning': 40,
    'info': 30,
    'debug': 20,
    'trace': 10,
}
"""Log levels are based off of the Bunyan format specification's log levels, not Python's."""


def _use_bunyan_structure(_: Any, _name: str, event_dict: EventDict) -> EventDict:  # noqa: ARG001
    """
    Match Bunyan format: https://github.com/trentm/node-bunyan?tab=readme-ov-file#core-fields
    """
    event_dict['msg'] = event_dict.pop('event')
    event_dict['v'] = 0
    event_dict['pid'] = os.getpid()
    event_dict['hostname'] = platform.node()
    event_dict['level'] = _BUNYAN_LOG_LEVELS[event_dict['level']]
    return event_dict


def _drop_color_message_key(_: Any, _name: str, event_dict: EventDict) -> EventDict:  # noqa: ARG001
    """
    Uvicorn logs the message a second time in the extra `color_message`, but we don't
    need it. This processor drops the key from the event dict if it exists.
    """
    event_dict.pop('color_message', None)
    return event_dict


def setup_logging() -> None:
    """
    This compels all loggers, including third-party loggers, to utilize a standard format with standard keys.

    Development mode wants a pretty format, while production mode will generally want to utilize formatted JSON logs in the Bunyan format.
    To inspect the Bunyan format, see https://github.com/trentm/node-bunyan?tab=readme-ov-file#core-fields

    Unless Uvicorn is not reloading AND has only one worker, you will need to call this function for each worker AND the main uvicorn runner.
    """
    use_json_logs = settings.PRODUCTION

    timestamper = structlog.processors.TimeStamper(fmt='iso', key='time')
    # add standard fields if the logger is not from structlog
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.processors.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.stdlib.ExtraAdder(),
        _drop_color_message_key,
        timestamper,
        structlog.processors.StackInfoRenderer(),
    ]

    if use_json_logs:
        shared_processors.append(_use_bunyan_structure)
        # Format the exception only for JSON logs
        shared_processors.append(structlog.processors.format_exc_info)

    structlog.configure(
        processors=[*shared_processors, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(settings.LOG_LEVEL),
        cache_logger_on_first_use=True,
    )

    formatter = (
        structlog.processors.JSONRenderer()
        if use_json_logs
        else structlog.dev.ConsoleRenderer(timestamp_key='time')
    )

    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'global': {
                    '()': structlog.stdlib.ProcessorFormatter,
                    'processors': [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        formatter,
                    ],
                    'foreign_pre_chain': shared_processors,
                },
            },
            'handlers': {
                'stderr': {
                    'level': settings.LOG_LEVEL,
                    'class': 'logging.StreamHandler',
                    'formatter': 'global',
                },
                #'queue_handler': {
                #'class': 'logging.handlers.QueueHandler',
                #'handlers': ['stderr'],
                #'respect_handler_level': True,
                # },
            },
            'loggers': {
                # root logger
                '': {
                    'handlers': ['stderr'],
                    #'handlers': ['queue_handler'],
                    'level': settings.LOG_LEVEL,
                    'propagate': True,
                },
                # we will override the uvicorn.access handler entirely
                'uvicorn.error': {
                    'handlers': ['stderr'],
                    #'handlers': ['queue_handler'],
                    'level': 'INFO',
                    'propagate': False,
                },
                'sqlalchemy.engine': {
                    'handlers': ['stderr'],
                    #'handlers': ['queue_handler'],
                    'level': 'INFO',
                    'propagate': False,
                },
                'sqlalchemy.pool': {
                    'handlers': ['stderr'],
                    #'handlers': ['queue_handler'],
                    'level': 'INFO',
                    'propagate': False,
                },
            },
        }
    )

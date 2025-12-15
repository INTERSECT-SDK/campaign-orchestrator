from typing import Annotated, Literal

from pydantic import BeforeValidator, Field, HttpUrl, PositiveInt
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from .definitions import (
    HIERARCHY_REGEX,
    BrokerProtocol,
)

LogLevel = Literal['CRITICAL', 'FATAL', 'ERROR', 'WARNING', 'WARN', 'INFO', 'DEBUG']


def strip_trailing_slash(value: str) -> str:
    value.rstrip('/')
    return value


StripTrailingSlash = Annotated[str, BeforeValidator(strip_trailing_slash)]


class Settings(BaseSettings):
    """variables which can be loaded as environment variables.

    For a valid local setup, see .env.example
    """

    ### GENERIC APP CONFIG ###

    LOG_LEVEL: LogLevel = Field(default='INFO')
    """Log level for the ENTIRE application"""
    PRODUCTION: bool = False
    """If True, this flag enables a few different settings:

    1) Binds to 0.0.0.0 instead of 127.0.0.1
    2) Format logs in JSON instead of the "pretty" format
    3) Turns off uvicorn reload
    4) Assumes that you are running Uvicorn behind a proxy, so starts passing through forwarded headers
    """

    SERVER_PORT: PositiveInt = 8000
    """The port Uvicorn will try to run on"""
    SERVER_WORKERS: PositiveInt = 1
    """Number of workers for Uvicorn."""
    BASE_URL: StripTrailingSlash = ''
    """Set this to '' if this is not behind a proxy, set this to your proxy's subpath if this is behind a proxy.

    Do not include the full URI, only include the path component.

    This is mostly used to make sure the generated API documentation links are correct. See the README for more information on how the proxy should be configured.
    """

    API_KEY: str = Field(default='X' * 32, min_length=32, max_length=255)
    """
    Key used to authorize access to the application. This should never be exposed to users directly, keep this in your backend.

    TODO - we may want to consider another approach, but this is a fairly critical exposure point, and we should require sane password lengths.
    """

    ### INTERSECT ###

    SYSTEM_NAME: str = Field(
        default='campaign-orchestrator-system', min_length=3, pattern=HIERARCHY_REGEX
    )
    """
    The System name is used as part of how INTERSECT clients know who to connect to, and can be shared with anyone.
    """

    # TODO - should allow for multiple brokers levels eventually.
    BROKER_HOST: str = 'localhost'
    BROKER_PORT: PositiveInt = 5672
    BROKER_PROTOCOL: BrokerProtocol = 'amqp0.9.1'
    """The protocol includes version information and will be used directly by Clients"""
    BROKER_TLS_CERT: str | None = None

    # These credentials are for the root broker. It is assumed that anyone publishing a message on this broker is allowed to by INTERSECT.
    BROKER_USERNAME: str = 'guest'
    BROKER_PASSWORD: str = 'guest'

    MINIO_URI: HttpUrl = 'http://localhost:9000'
    """(should include port)"""
    MINIO_USERNAME: str = 'minioadmin'
    MINIO_PASSWORD: str = 'minioadmin'

    # pydantic config, NOT an environment variable
    model_config = SettingsConfigDict(
        case_sensitive=True,
        frozen=True,
        cli_parse_args=False,  # Disabled to avoid conflicts with pytest CLI parsing
        env_file='.env',  # not used in production; this only needs to exist if you don't have environment variables already set
        extra='ignore',
        validate_default=False,  # I have no idea why pydantic-settings overrides pydantic's default, but we don't need it
        env_ignore_empty=True,  # treat empty ENV strings as None unless the value explicitly defaults to empty string
    )


settings = Settings()

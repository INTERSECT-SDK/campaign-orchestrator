"""Shared fixtures and helpers for integration tests."""

from __future__ import annotations

import json
import os
import pathlib
import socket
import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
    from intersect_orchestrator.app.core.intersect_client import CoreServiceIntersectClient
    from intersect_orchestrator.app.core.repository import (
        MongoCampaignRepository,
        PostgresCampaignRepository,
    )

TEST_DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
CAMPAIGN_FILE = TEST_DATA_DIR / 'campaign' / 'random-number-campaign.campaign.json'


# ============================================================================
# Service Availability Helpers
# ============================================================================


def is_broker_available() -> bool:
    """Check if the RabbitMQ broker is available."""
    broker_host = os.getenv('BROKER_HOST', 'localhost')
    broker_port = int(os.getenv('BROKER_PORT', '5672'))

    try:
        with socket.create_connection((broker_host, broker_port), timeout=2):
            return True
    except (TimeoutError, ConnectionRefusedError, OSError):
        return False


def is_orchestrator_available() -> bool:
    """Check if the orchestrator REST API is available."""
    orchestrator_host = os.getenv('ORCHESTRATOR_HOST', 'localhost')
    orchestrator_port = int(os.getenv('ORCHESTRATOR_PORT', '8000'))

    try:
        with socket.create_connection((orchestrator_host, orchestrator_port), timeout=2):
            return True
    except (TimeoutError, ConnectionRefusedError, OSError):
        return False


def _wait_for_mongo(uri: str, timeout: float = 10.0) -> None:
    """Wait for MongoDB to become available."""
    try:
        import pymongo
    except ImportError:
        pytest.skip('pymongo not installed - skipping MongoDB tests')

    start = time.time()
    while time.time() - start < timeout:
        try:
            client = pymongo.MongoClient(uri)
            client.admin.command('ping')
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
        else:
            return
    pytest.skip('MongoDB not available for integration tests')


def _wait_for_postgres(dsn: str, timeout: float = 10.0) -> None:
    """Wait for PostgreSQL to become available."""
    try:
        import psycopg
    except ImportError:
        pytest.skip('psycopg not installed - skipping PostgreSQL tests')

    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg.connect(dsn)
            conn.execute('SELECT 1')
            conn.close()
        except Exception:  # noqa: BLE001
            time.sleep(0.5)
        else:
            return
    pytest.skip('PostgreSQL not available for integration tests')


# ============================================================================
# Service Availability Fixtures
# ============================================================================


@pytest.fixture(scope='session')
def check_broker_available() -> None:
    """Check broker availability and skip tests if unavailable.

    Tests that need the broker should use this fixture explicitly.
    Tests will be skipped if the broker is not running.
    """
    if not is_broker_available():
        pytest.skip(
            f'RabbitMQ broker not available at '
            f'{os.getenv("BROKER_HOST", "localhost")}:{os.getenv("BROKER_PORT", "5672")}. '
            f"Run 'docker-compose up -d' to start the broker."
        )


@pytest.fixture(scope='session')
def check_orchestrator_available() -> None:
    """Check orchestrator availability and skip tests if unavailable.

    Tests that need the orchestrator should use this fixture explicitly.
    Tests will be skipped if the orchestrator is not running.
    """
    if not is_orchestrator_available():
        pytest.skip(
            f'Campaign orchestrator not available at '
            f'{os.getenv("ORCHESTRATOR_HOST", "localhost")}:{os.getenv("ORCHESTRATOR_PORT", "8000")}. '
            f"Run 'docker-compose up' to start all services."
        )


# ============================================================================
# Data Loading Helpers
# ============================================================================


def load_campaign_json() -> dict[str, Any]:
    """Load campaign JSON from test data."""
    with CAMPAIGN_FILE.open() as f:
        return json.load(f)


# ============================================================================
# URL and Configuration Helpers
# ============================================================================


def get_orchestrator_url() -> str:
    """Get the base HTTP URL for the orchestrator REST API."""
    host = os.getenv('ORCHESTRATOR_HOST', 'localhost')
    port = os.getenv('ORCHESTRATOR_PORT', '8000')
    return f'http://{host}:{port}'


def get_orchestrator_ws_url() -> str:
    """Get the WebSocket URL for the orchestrator."""
    host = os.getenv('ORCHESTRATOR_HOST', 'localhost')
    port = os.getenv('ORCHESTRATOR_PORT', '8000')
    return f'ws://{host}:{port}'


def get_api_key() -> str:
    """Get the API key for authentication."""
    return os.getenv('API_KEY', 'test-api-key-12345678901234567890')


def create_intersect_client() -> CoreServiceIntersectClient:
    """Create a real CoreServiceIntersectClient connected to the broker.

    This creates a client configured to connect to the broker from docker-compose,
    using environment variables or sensible defaults.
    """
    from intersect_orchestrator.app.core.environment import Settings
    from intersect_orchestrator.app.core.intersect_client import CoreServiceIntersectClient

    # Get broker config from environment or use docker-compose defaults
    broker_host = os.getenv('BROKER_HOST', 'localhost')
    broker_port = int(os.getenv('BROKER_PORT', '5672'))
    broker_username = os.getenv('BROKER_USERNAME', 'intersect_username')
    broker_password = os.getenv('BROKER_PASSWORD', 'intersect_password')
    broker_protocol = os.getenv('BROKER_PROTOCOL', 'amqp0.9.1')

    # Create settings with broker config
    settings = Settings(
        BROKER_HOST=broker_host,
        BROKER_PORT=broker_port,
        BROKER_USERNAME=broker_username,
        BROKER_PASSWORD=broker_password,
        BROKER_PROTOCOL=broker_protocol,
    )

    # Create and return the client
    client = CoreServiceIntersectClient(settings)
    return client


# ============================================================================
# Repository Fixtures
# ============================================================================


@pytest.fixture(scope='session')
def mongo_uri() -> str:
    """Get MongoDB connection URI from environment."""
    return os.getenv(
        'CAMPAIGN_REPOSITORY_MONGO_URI',
        'mongodb://intersect:intersect@localhost:27017/?authSource=admin',
    )


@pytest.fixture(scope='session')
def mongo_db() -> str:
    """Get MongoDB database name from environment."""
    return os.getenv('CAMPAIGN_REPOSITORY_MONGO_DB', 'intersect_orchestrator')


@pytest.fixture(scope='session')
def postgres_dsn() -> str:
    """Get PostgreSQL connection DSN from environment."""
    return os.getenv(
        'CAMPAIGN_REPOSITORY_POSTGRES_DSN',
        'postgresql://intersect:intersect@localhost:5432/intersect_orchestrator',
    )


@pytest.fixture
def repository_mongo(mongo_uri: str, mongo_db: str) -> MongoCampaignRepository:
    """Create a MongoDB repository instance for testing.

    This fixture waits for MongoDB to be available and returns a configured
    repository instance.
    """
    _wait_for_mongo(mongo_uri)
    import pymongo

    client = pymongo.MongoClient(mongo_uri)
    from intersect_orchestrator.app.core.repository import MongoCampaignRepository

    return MongoCampaignRepository(client, db_name=mongo_db)


@pytest.fixture
def repository_postgres(postgres_dsn: str) -> PostgresCampaignRepository:
    """Create a PostgreSQL repository instance for testing.

    This fixture waits for PostgreSQL to be available and returns a configured
    repository instance.
    """
    _wait_for_postgres(postgres_dsn)
    import psycopg

    conn = psycopg.connect(postgres_dsn)
    conn.execute("SET TIME ZONE 'UTC'")
    from intersect_orchestrator.app.core.repository import PostgresCampaignRepository

    return PostgresCampaignRepository(conn)


# ============================================================================
# Test Data Fixtures
# ============================================================================


@pytest.fixture
def simple_campaign() -> Campaign:
    """Create a simple campaign for repository testing."""
    from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
        Campaign,
        Task,
        TaskGroup,
    )

    return Campaign(
        id='campaign-repo-1',
        name='Repo Campaign',
        user='tester',
        description='Repo campaign description',
        task_groups=[
            TaskGroup(
                id='tg-1',
                group_dependencies=[],
                tasks=[
                    Task(
                        id='task-1',
                        hierarchy='capability',
                        capability='capability-1',
                        operation_id='op-1',
                    )
                ],
            )
        ],
    )

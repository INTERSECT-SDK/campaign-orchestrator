"""End-to-end test for campaign execution with random number service.

These tests require a running RabbitMQ broker. Start the broker with:
    docker-compose up -d

If the broker is not available, tests will be skipped gracefully.
"""

from __future__ import annotations

import json
import os
import pathlib
import socket
import uuid

import pytest

TEST_DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
CAMPAIGN_FILE = TEST_DATA_DIR / 'campaign' / 'random-number-campaign.campaign.json'


def is_broker_available() -> bool:
    """Check if the broker is available."""
    broker_host = os.getenv('BROKER_HOST', 'localhost')
    broker_port = int(os.getenv('BROKER_PORT', '5672'))

    try:
        with socket.create_connection((broker_host, broker_port), timeout=2):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


@pytest.fixture(scope='session', autouse=True)
def check_broker_available() -> None:
    """Check broker availability and skip tests if unavailable."""
    if not is_broker_available():
        pytest.skip(
            f'RabbitMQ broker not available at '
            f'{os.getenv("BROKER_HOST", "localhost")}:{os.getenv("BROKER_PORT", "5672")}. '
            f"Run 'docker-compose up -d' to start the broker."
        )


def load_campaign_json() -> dict:
    """Load campaign JSON from test data."""
    with CAMPAIGN_FILE.open() as f:
        return json.load(f)


def create_intersect_client() -> 'CoreServiceIntersectClient':  # noqa: F821
    """Create a real CoreServiceIntersectClient connected to the broker from docker-compose."""
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


@pytest.mark.integration
class TestCampaignE2E:
    """End-to-end tests for campaign execution."""

    def test_submit_campaign_creates_state_and_petri_net(self) -> None:
        """Test that submitting a campaign creates state and Petri Net using real broker."""
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
            Campaign,
        )
        from intersect_orchestrator.app.core.campaign_orchestrator import (
            CampaignOrchestrator,
        )
        from intersect_orchestrator.app.core.repository import (
            InMemoryCampaignRepository,
        )

        # Load campaign
        campaign_data = load_campaign_json()
        campaign = Campaign(**campaign_data)

        # Create orchestrator with real client and in-memory repository
        repository = InMemoryCampaignRepository()
        real_client = create_intersect_client()
        orchestrator = CampaignOrchestrator(intersect_client=real_client, repository=repository)

        # Submit campaign
        campaign_id = orchestrator.submit_campaign(campaign)
        assert campaign_id is not None
        assert isinstance(campaign_id, uuid.UUID)

        # Verify campaign was stored at some point (may have been removed if already completed)
        # We check the repository instead
        stored_state = repository.load_snapshot(campaign_id)
        assert stored_state is not None
        assert stored_state.campaign_id == campaign_id

        # Verify events were recorded
        events = list(repository.load_events(campaign_id))
        assert len(events) > 0

    def test_campaign_state_progression(self) -> None:
        """Test that campaign state progresses through execution using real broker."""
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
            Campaign,
        )
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
            ExecutionStatus,
        )
        from intersect_orchestrator.app.core.campaign_orchestrator import (
            CampaignOrchestrator,
        )
        from intersect_orchestrator.app.core.repository import (
            InMemoryCampaignRepository,
        )

        # Load campaign
        campaign_data = load_campaign_json()
        campaign = Campaign(**campaign_data)

        # Create orchestrator with real client
        repository = InMemoryCampaignRepository()
        real_client = create_intersect_client()
        orchestrator = CampaignOrchestrator(intersect_client=real_client, repository=repository)

        # Submit campaign
        campaign_id = orchestrator.submit_campaign(campaign)

        # Verify final state is recorded in repository
        state_snapshot = repository.load_snapshot(campaign_id)
        assert state_snapshot is not None
        assert state_snapshot.state.status in [
            ExecutionStatus.QUEUED,
            ExecutionStatus.RUNNING,
            ExecutionStatus.COMPLETE,
        ]

        # Verify task groups exist
        assert len(state_snapshot.state.task_groups) > 0
        for task_group in state_snapshot.state.task_groups:
            assert task_group.id is not None
            assert len(task_group.tasks) > 0

    def test_campaign_events_recorded(self) -> None:
        """Test that campaign events are properly recorded in repository using real broker."""
        from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
            Campaign,
        )
        from intersect_orchestrator.app.core.campaign_orchestrator import (
            CampaignOrchestrator,
        )
        from intersect_orchestrator.app.core.repository import (
            InMemoryCampaignRepository,
        )

        # Load campaign
        campaign_data = load_campaign_json()
        campaign = Campaign(**campaign_data)

        # Create orchestrator with real client
        repository = InMemoryCampaignRepository()
        real_client = create_intersect_client()
        orchestrator = CampaignOrchestrator(intersect_client=real_client, repository=repository)

        # Submit campaign
        campaign_id = orchestrator.submit_campaign(campaign)

        # Load events from repository
        events = list(repository.load_events(campaign_id))

        # Verify events have proper structure
        assert len(events) > 0
        for event in events:
            assert event.campaign_id == campaign_id
            assert event.event_id is not None
            assert event.seq > 0
            assert event.event_type is not None
            assert event.timestamp is not None

        # Verify no duplicate seq numbers (this was a bug we fixed)
        seqs = [event.seq for event in events]
        assert len(seqs) == len(set(seqs)), 'Events must have unique seq numbers'

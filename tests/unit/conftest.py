import json
import pathlib
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator
from intersect_orchestrator.app.core.environment import settings
from intersect_orchestrator.app.core.intersect_client import CoreServiceIntersectClient
from tests import TEST_DATA_DIR


# Create pytest hook to set up mocking before any modules are imported
def pytest_configure(config):
    """Set up mocking before test collection."""
    from unittest.mock import patch

    # Mock the ControlPlaneManager module before anything imports it
    mock_manager_instance = MagicMock()
    mock_manager_instance.is_connected.return_value = True
    mock_manager_instance.considered_unrecoverable.return_value = False
    mock_manager_instance.connect.return_value = None
    mock_manager_instance.add_subscription_channel.return_value = None
    mock_manager_instance.disconnect.return_value = None

    # Patch at the module level
    patcher = patch(
        'intersect_orchestrator.app.intersect_control_plane_fork.control_plane_manager.ControlPlaneManager',
        return_value=mock_manager_instance,
    )
    patcher.start()
    config._mock_patcher = patcher


# Now import app after the hook runs
from intersect_orchestrator.app.main import app  # noqa: E402


@pytest.fixture
def random_number_campaign_petri_net():
    """
    Campaign of single node Petri Net for random number service.
    """
    return pathlib.Path(TEST_DATA_DIR, 'petri_nets_yaml', 'random-number-workflow.yaml')


@pytest.fixture
def random_number_and_histogram_campaign_petri_net():
    """
    Campaign of Petri Net with nodes for random number service and histogram viz.
    """
    return pathlib.Path(
        TEST_DATA_DIR, 'petri_nets_yaml', 'random-number-and-histogram-workflow.yaml'
    )


@pytest.fixture
def random_number_campaign_data():
    """Sample campaign data for the random number campaign."""
    campaign_path = pathlib.Path(TEST_DATA_DIR, 'campaign', 'random-number-campaign.campaign.json')
    with campaign_path.open() as f:
        data = json.load(f)

    # Validate using the Campaign model
    Campaign(**data)
    return data


@pytest.fixture
def random_number_and_histogram_campaign_data():
    """Sample campaign data for the random number + histogram campaign."""
    campaign_path = pathlib.Path(
        TEST_DATA_DIR, 'campaign', 'random-number-and-histogram-campaign.campaign.json'
    )
    with campaign_path.open() as f:
        data = json.load(f)

    # Validate using the Campaign model
    Campaign(**data)
    return data


@pytest.fixture
def sample_campaign_data(random_number_campaign_data):
    """Default campaign payload for API tests."""
    return random_number_campaign_data


@pytest.fixture
def campaign_payloads():
    """Load all campaign payloads from test data."""
    campaign_dir = pathlib.Path(TEST_DATA_DIR, 'campaign')
    payloads = []
    for campaign_path in sorted(campaign_dir.glob('*.campaign.json')):
        with campaign_path.open() as f:
            data = json.load(f)
        Campaign(**data)
        payloads.append(data)
    return payloads


@pytest.fixture
def client():
    """FastAPI test client with properly initialized app state."""
    # Create a mock client with mocked broker
    mock_client = MagicMock(spec=CoreServiceIntersectClient)
    mock_client.is_connected.return_value = True
    mock_client.can_reconnect.return_value = True
    mock_client.broadcast_message.return_value = None
    mock_client.add_http_connection.return_value = MagicMock()

    # Create the orchestrator
    orchestrator = CampaignOrchestrator(mock_client)

    # Manually set the app state since lifespan doesn't run with TestClient
    app.state.intersect_client = mock_client
    app.state.campaign_orchestrator = orchestrator
    mock_client.set_campaign_orchestrator(orchestrator)

    return TestClient(app)


@pytest.fixture
def valid_api_key():
    """Valid API key for testing."""
    return settings.API_KEY


@pytest.fixture
def invalid_api_key():
    """Invalid API key for testing."""
    return 'invalid_key'

import json
import pathlib

import pytest
from fastapi.testclient import TestClient

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
from intersect_orchestrator.app.core.environment import settings
from intersect_orchestrator.app.main import app

from . import TEST_DATA_DIR


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
def client():
    """FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def valid_api_key():
    """Valid API key for testing."""
    return settings.API_KEY


@pytest.fixture
def invalid_api_key():
    """Invalid API key for testing."""
    return 'invalid_key'

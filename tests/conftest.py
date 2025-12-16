import pathlib
import uuid

import pytest
from fastapi.testclient import TestClient

from intersect_orchestrator.app.core.environment import settings
from intersect_orchestrator.app.main import app

from . import TEST_DATA_DIR


@pytest.fixture
def random_number_campaign_icmp():
    """
    Campaign of single node ICMP for random number service.
    """
    return pathlib.Path(TEST_DATA_DIR, 'icmp', 'random-number-campaign.icmp')


@pytest.fixture
def random_number_and_histogram_campaign_icmp():
    """
    Campaign of ICMP with nodes for random number service and histogram viz.
    """
    return pathlib.Path(TEST_DATA_DIR, 'icmp', 'random-number-and-histogram-campaign.icmp')


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


@pytest.fixture
def sample_icmp_data():
    """Sample ICMP data for testing."""
    return {
        'campaignId': 'test-campaign-123',
        'campaignName': 'Test Campaign',
        'nodes': [
            {
                'id': str(uuid.uuid4()),
                'type': 'capability',
                'data': {
                    'capability': {
                        'name': 'TestCapability',
                        'created_at': '2025-12-16T10:00:00.000Z',
                        'last_lifecycle_message': None,
                        'service_id': 1,
                        'endpoints_schema': {
                            'channels': {
                                'test_operation': {
                                    'publish': {
                                        'message': {
                                            'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                                            'contentType': 'application/json',
                                            'traits': {
                                                '$ref': '#/components/messageTraits/commonHeaders'
                                            },
                                            'payload': {
                                                'type': 'object',
                                                'properties': {'result': {'type': 'string'}},
                                            },
                                        },
                                        'description': 'Test operation',
                                    },
                                    'subscribe': {
                                        'message': {
                                            'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                                            'contentType': 'application/json',
                                            'traits': {
                                                '$ref': '#/components/messageTraits/commonHeaders'
                                            },
                                            'payload': {
                                                'type': 'object',
                                                'properties': {'input': {'type': 'string'}},
                                            },
                                        },
                                        'description': 'Test operation',
                                    },
                                    'events': [],
                                }
                            }
                        },
                    },
                    'endpoint': 'test_operation',
                    'endpoint_channel': {},
                },
            }
        ],
        'edges': [],
        'metadata': {},
    }

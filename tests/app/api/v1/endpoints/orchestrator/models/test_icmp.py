import json
import pytest
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.icmp import Icmp


def test_random_number_campaign(random_number_campaign_icmp):
    """Test that the icmp files can be loaded and validated using the Icmp model."""
    with open(random_number_campaign_icmp, 'r') as f:
        data = json.load(f)
    
    # This should not raise an exception if the model matches the data
    icmp = Icmp(**data)
    
    # Basic assertions
    assert isinstance(icmp.nodes, list)
    assert len(icmp.nodes) == 1
    assert isinstance(icmp.edges, list)
    assert isinstance(icmp.metadata, dict)
    
    # Check node types
    node = icmp.nodes[0]
    assert node.type == 'capability'
    assert hasattr(node.data, 'capability')
    assert hasattr(node.data, 'endpoint')
    assert hasattr(node.data, 'endpoint_channel')


def test_random_number_and_histogram_campaign(random_number_and_histogram_campaign_icmp):
    """Test that the histogram icmp file can be loaded and validated using the Icmp model."""
    with open(random_number_and_histogram_campaign_icmp, 'r') as f:
        data = json.load(f)
    
    # This should not raise an exception if the model matches the data
    icmp = Icmp(**data)
    
    # Basic assertions
    assert isinstance(icmp.nodes, list)
    assert len(icmp.nodes) == 2
    assert isinstance(icmp.edges, list)
    assert isinstance(icmp.metadata, dict)
    
    # Check node types
    capability_node = next(node for node in icmp.nodes if node.type == 'capability')
    visualization_node = next(node for node in icmp.nodes if node.type == 'visualization')
    
    # Check capability node
    assert hasattr(capability_node.data, 'capability')
    assert hasattr(capability_node.data, 'endpoint')
    assert hasattr(capability_node.data, 'endpoint_channel')
    
    # Check visualization node
    assert hasattr(visualization_node.data, 'type')
    assert hasattr(visualization_node.data, 'name')
    assert hasattr(visualization_node.data, 'spec')
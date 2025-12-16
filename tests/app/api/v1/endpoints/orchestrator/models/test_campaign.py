import json
import pytest
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.icmp import Icmp
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
from intersect_orchestrator.app.utils.conversions import icmp_to_campaign, campaign_to_icmp


def test_random_number_campaign_conversion(random_number_campaign_icmp):
    """Test conversion of single-node random number campaign."""
    with open(random_number_campaign_icmp, 'r') as f:
        icmp_data = json.load(f)

    campaign = icmp_to_campaign(icmp_data)

    assert isinstance(campaign, Campaign)
    assert campaign.id == icmp_data['campaignId']
    assert campaign.name == icmp_data['campaignName']
    assert len(campaign.task_groups) == 1  # Single capability node

    # Check task group structure
    task_group = campaign.task_groups[0]
    assert len(task_group.tasks) == 1
    assert task_group.tasks[0].id == 'generate_random_number'


def test_histogram_campaign_conversion(random_number_and_histogram_campaign_icmp):
    """Test conversion of two-node campaign with edge."""
    with open(random_number_and_histogram_campaign_icmp, 'r') as f:
        icmp_data = json.load(f)

    campaign = icmp_to_campaign(icmp_data)

    assert isinstance(campaign, Campaign)
    assert campaign.id == icmp_data['campaignId']
    assert campaign.name == icmp_data['campaignName']
    assert len(campaign.task_groups) == 2  # Two nodes: capability + visualization

    # Check that tasks are properly connected
    task_groups = {tg.id: tg for tg in campaign.task_groups}
    assert 'capability_group' in task_groups
    assert 'visualization_group' in task_groups

    # Check dependencies
    viz_group = task_groups['visualization_group']
    assert 'capability_group' in viz_group.group_dependencies


def test_campaign_validation(random_number_campaign_icmp):
    """Test that converted campaign passes validation."""
    with open(random_number_campaign_icmp, 'r') as f:
        icmp_data = json.load(f)

    campaign = icmp_to_campaign(icmp_data)
    campaign_name = icmp_data.get('campaignName', 'Converted Campaign')

    # This should not raise an exception if the conversion is valid
    assert campaign.id == icmp_data['campaignId']
    assert campaign.name == campaign_name
    assert len(campaign.task_groups) == 1
    assert campaign.task_groups[0].id == 'capability_group'


def test_random_number_campaign_to_icmp_conversion(random_number_campaign_icmp):
    """Test conversion of single-node random number campaign back to ICMP."""
    with open(random_number_campaign_icmp, 'r') as f:
        original_icmp_data = json.load(f)

    # First convert to Campaign
    campaign = icmp_to_campaign(original_icmp_data)

    # Then convert back to ICMP
    icmp_data = campaign_to_icmp(campaign)

    # Validate the converted ICMP data
    assert isinstance(icmp_data, dict)
    assert icmp_data['campaignId'] == original_icmp_data['campaignId']
    assert icmp_data['campaignName'] == original_icmp_data['campaignName']
    assert len(icmp_data['nodes']) == 1  # Single capability node
    assert len(icmp_data['edges']) == 0  # No edges in single node campaign

    # Check node structure
    node = icmp_data['nodes'][0]
    assert node['type'] == 'capability'
    assert 'data' in node
    assert 'capability' in node['data']
    assert node['data']['endpoint'] == 'generate_random_number'


def test_histogram_campaign_to_icmp_conversion(random_number_and_histogram_campaign_icmp):
    """Test conversion of two-node campaign with edge back to ICMP."""
    with open(random_number_and_histogram_campaign_icmp, 'r') as f:
        original_icmp_data = json.load(f)

    # First convert to Campaign
    campaign = icmp_to_campaign(original_icmp_data)

    # Then convert back to ICMP
    icmp_data = campaign_to_icmp(campaign)

    # Validate the converted ICMP data
    assert isinstance(icmp_data, dict)
    assert icmp_data['campaignId'] == original_icmp_data['campaignId']
    assert icmp_data['campaignName'] == original_icmp_data['campaignName']
    assert len(icmp_data['nodes']) == 2  # Two nodes: capability + visualization
    assert len(icmp_data['edges']) == 1  # One edge between nodes

    # Check node types
    node_types = [node['type'] for node in icmp_data['nodes']]
    assert 'capability' in node_types
    assert 'visualization' in node_types

    # Check edge structure
    edge = icmp_data['edges'][0]
    assert 'source' in edge
    assert 'target' in edge
    assert edge['type'] == 'baseCampaignEdge'


def test_round_trip_conversion(random_number_campaign_icmp):
    """Test that ICMP -> Campaign -> ICMP round trip preserves key information."""
    with open(random_number_campaign_icmp, 'r') as f:
        original_icmp_data = json.load(f)

    # Convert ICMP -> Campaign -> ICMP
    campaign = icmp_to_campaign(original_icmp_data)
    icmp_data = campaign_to_icmp(campaign)

    # Key information should be preserved
    assert icmp_data['campaignId'] == original_icmp_data['campaignId']
    assert icmp_data['campaignName'] == original_icmp_data['campaignName']
    assert len(icmp_data['nodes']) == len(original_icmp_data['nodes'])

    # Node types should be preserved
    original_types = {node['type'] for node in original_icmp_data['nodes']}
    converted_types = {node['type'] for node in icmp_data['nodes']}
    assert original_types == converted_types

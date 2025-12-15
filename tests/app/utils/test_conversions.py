import json
from pathlib import Path
import pytest
import yaml
from intersect_orchestrator.app.utils.conversions import icmp_to_petri_net, petri_net_to_icmp


class TestIcmpToPetriNet:
    """Test conversion from ICMP JSON to Petri net YAML format."""

    def test_random_number_campaign_icmp_to_petri_conversion(self, random_number_campaign_icmp):
        """Test conversion of single-node random number campaign."""
        with open(random_number_campaign_icmp, 'r') as f:
            icmp_data = json.load(f)

        petri_net = icmp_to_petri_net(icmp_data)

        assert petri_net['net_name'] == 'RandomNumberWorkflow'
        assert 'Ready' in petri_net['places']
        assert 'Complete' in petri_net['places']
        assert len(petri_net['transitions']) == 1

        transition = petri_net['transitions'][0]
        assert transition['name'] == 'RandomNumberGenerator'
        assert transition['inputs'] == ['Ready']
        assert transition['outputs'] == ['Complete']
        assert 'publish_topic' in transition
        assert 'subscribe_topic' in transition

    def test_random_number_histogram_campaign_icmp_to_petri_conversion(self, random_number_and_histogram_campaign_icmp):
        """Test conversion of two-node campaign with edge."""
        with open(random_number_and_histogram_campaign_icmp, 'r') as f:
            icmp_data = json.load(f)

        petri_net = icmp_to_petri_net(icmp_data)

        assert petri_net['net_name'] == 'RandomNumberAndHistogramWorkflow'
        assert len(petri_net['places']) >= 3  # Ready, intermediate, Complete
        assert len(petri_net['transitions']) >= 2  # At least 2 transitions

        # Check that transitions are properly connected
        transition_names = [t['name'] for t in petri_net['transitions']]
        assert 'RandomNumberGenerator' in transition_names
        assert any('Histogram' in name for name in transition_names)


class TestPetriNetToIcmp:
    """Test conversion from Petri net YAML to ICMP JSON format."""

    def test_random_number_campaign_petri_to_icmp_conversion(self, random_number_campaign_petri_net):
        """Test conversion of simple petri net to ICMP."""
        with open(random_number_campaign_petri_net, 'r') as f:
            petri_net = yaml.safe_load(f)

        icmp_data = petri_net_to_icmp(petri_net)

        assert icmp_data['campaignId'].startswith('Campaign-')
        assert icmp_data['campaignName'] == 'RandomNumberWorkflow'
        assert len(icmp_data['nodes']) == 1
        assert len(icmp_data['edges']) == 0  # Single transition, no edges needed

        node = icmp_data['nodes'][0]
        assert node['type'] == 'capability'
        assert 'capability' in node['data']
        assert node['data']['capability']['name'] == 'Random_Number_Generator'

    def test_random_number_histogram_campaign_petri_to_icmp_conversion(self, random_number_and_histogram_campaign_petri_net):
        """Test conversion of multi-transition petri net."""
        with open(random_number_and_histogram_campaign_petri_net, 'r') as f:
            petri_net = yaml.safe_load(f)

        icmp_data = petri_net_to_icmp(petri_net)

        assert icmp_data['campaignName'] == 'RandomNumberAndHistogramWorkflow'
        assert len(icmp_data['nodes']) == 3  # Three transitions = three nodes
        assert len(icmp_data['edges']) == 2  # Two connections between nodes

        # Check that nodes have proper capability data
        for node in icmp_data['nodes']:
            assert node['type'] == 'capability'
            assert 'capability' in node['data']
            assert 'endpoint' in node['data']
            assert 'endpoint_channel' in node['data']

    def test_round_trip_conversion(self, random_number_campaign_icmp):
        """Test that ICMP -> Petri net -> ICMP produces equivalent structure."""
        with open(random_number_campaign_icmp, 'r') as f:
            original_icmp = json.load(f)

        # Convert to petri net and back
        petri_net = icmp_to_petri_net(original_icmp)
        converted_icmp = petri_net_to_icmp(petri_net)

        # Check that key structure is preserved
        assert converted_icmp['campaignName'] == petri_net['net_name']
        assert len(converted_icmp['nodes']) == len(petri_net['transitions'])
        assert 'campaignId' in converted_icmp
        assert 'createdAt' in converted_icmp
        assert 'updatedAt' in converted_icmp
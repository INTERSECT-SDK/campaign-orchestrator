import pathlib

import pytest

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

import pathlib

import pytest
from . import TEST_DATA_DIR

@pytest.fixture
def random_number_campaign_icmp():
    """
    Campaign of single node ICMP for random number service.
    """
    p = pathlib.Path(TEST_DATA_DIR, "random-number-campaign.icmp")
    return p


@pytest.fixture
def random_number_and_histogram_campaign_icmp():
    """
    Campaign of ICMP with nodes for random number service and histogram viz.
    """
    p = pathlib.Path(TEST_DATA_DIR, "random-number-and-histogram-campaign.icmp")
    return p

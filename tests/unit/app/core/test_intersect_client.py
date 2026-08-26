"""Tests for the orchestrator's INTERSECT control-plane channel wiring."""

import re
from unittest.mock import MagicMock, patch

import pytest
from intersect_sdk_common.control_plane.control_plane_manager import _CHANNEL_REGEX

from intersect_orchestrator.app.core.environment import Settings
from intersect_orchestrator.app.core.intersect_client import (
    EVENT_WILDCARD_CHANNEL,
    CoreServiceIntersectClient,
)

# An INTERSECT hierarchy is always five parts (subsystem is rendered as '-' when
# unset), and services publish events on '<hierarchy>/events/<capability>/<event>'.
_EXAMPLE_EVENT_CHANNEL = (
    'random-organization/random-facility/random-system/random-subsystem/'
    'random-service/events/RandomNumberGenerator/newMeasurement'
)


def _wildcard_to_regex(channel: str) -> re.Pattern[str]:
    """Translate a protocol-agnostic INTERSECT channel into a matcher.

    Follows the convention documented on ``ControlPlaneManager.add_subscription_channel``:
    '/' separates words, '*' matches exactly one word, '#' matches any number of
    trailing words.
    """
    parts = []
    for word in channel.split('/'):
        if word == '*':
            parts.append(r'[^/]+')
        elif word == '#':
            parts.append(r'.+')
        else:
            parts.append(re.escape(word))
    return re.compile(rf'^{"/".join(parts)}$')


def test_event_wildcard_channel_is_accepted_by_the_sdk() -> None:
    """The SDK rejects channels that aren't alphanumeric plus '* # / -'."""
    assert _CHANNEL_REGEX.match(EVENT_WILDCARD_CHANNEL)


def test_event_wildcard_channel_matches_a_service_event_channel() -> None:
    assert _wildcard_to_regex(EVENT_WILDCARD_CHANNEL).match(_EXAMPLE_EVENT_CHANNEL)


@pytest.mark.parametrize(
    'channel',
    [
        'random-organization/random-facility/random-system/random-subsystem/random-service/request',
        'random-organization/random-facility/random-system/random-subsystem/random-service/response',
        'random-organization/random-facility/random-system/random-subsystem/random-service/lifecycle',
    ],
)
def test_event_wildcard_channel_ignores_non_event_channels(channel: str) -> None:
    """The event subscription must not swallow request/response/lifecycle traffic."""
    assert not _wildcard_to_regex(EVENT_WILDCARD_CHANNEL).match(channel)


def test_client_subscribes_to_the_event_wildcard_channel() -> None:
    manager = MagicMock()
    with patch(
        'intersect_orchestrator.app.core.intersect_client.ControlPlaneManager',
        return_value=manager,
    ):
        CoreServiceIntersectClient(Settings())

    subscribed_channels = [call.args[0] for call in manager.add_subscription_channel.call_args_list]
    assert EVENT_WILDCARD_CHANNEL in subscribed_channels

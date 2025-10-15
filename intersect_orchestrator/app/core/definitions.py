"""This module consists of core definitions which are meant to transcend the entire application"""

from typing import Literal

BrokerProtocol = Literal['amqp0.9.1', 'mqtt5.0']
"""PubSub protocols we support."""

INTERSECT_MESSAGE_EXCHANGE = 'intersect-messages'
"""Currently, this is just used for the name of the message exchange on RabbitMQ."""

INTERSECT_SERVICE_SUBSCRIPTION_TYPES = (
    'request',
    'response',
)
"""SDK Services will need to subscribe to these queues, and only the SDK Service is allowed to subscribe to these queues."""

INTERSECT_SERVICE_PUBLISH_TYPES = (
    'events',
    'lifecycle',
)
"""SDK Services are the only broker users allowed to publish to these queues."""

INTERSECT_CLIENT_MESSAGE_TYPES = ('response',)
"""SDK Clients will need their own dedicated queues associated for these message types."""


def get_raw_protocol(proto: BrokerProtocol, tls: bool = False) -> str:
    """strip protocol version and handle TLS changes"""
    if proto == 'amqp0.9.1':
        return 'amqps' if tls else 'amqp'
    if proto == 'mqtt5.0':
        return 'mqtts' if tls else 'mqtt'
    msg = f'Unsupported proto {proto}'
    raise ValueError(msg)


def get_uri_path(proto: BrokerProtocol) -> str:
    if proto == 'amqp0.9.1':
        # use the '/' virtual host for every single message
        return '/%2F'
    if proto == 'mqtt5.0':
        # no path options
        return '/'
    msg = f'Unsupported proto {proto}'
    raise ValueError(msg)


HIERARCHY_REGEX = r'[a-z0-9][-a-z0-9]{2,62}'
"""Regex we permit for the System name (defined by us) and the Service names (requested by users)"""

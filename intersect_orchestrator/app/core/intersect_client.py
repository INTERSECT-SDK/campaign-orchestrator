"""
PLEASE NOTE: This file contains internal intersect-sdk library imports.

The INTERSECT-SDK library does NOT promise that internal APIs will not change in "non-breaking" releases.
At time of writing (2024-11-07), it is not anticipated that the used internal APIs will change.

These library interactions cover:
  - expected INTERSECT message structures (not worthwhile to duplicate)
  - simplified APIs for interacting with protocols, etc. (could potentially be duplicated)

"""

import logging
from asyncio import Queue
from typing import TYPE_CHECKING

from ..intersect_control_plane_fork.control_plane_manager import (
    ControlPlaneConfig,
    ControlPlaneManager,
)
from .environment import Settings

"""
from pydantic import TypeAdapter, ValidationError

from intersect_sdk._internal.messages.event import EventMessage
from intersect_sdk._internal.messages.lifecycle import LifecycleMessage
from intersect_sdk._internal.messages.userspace import UserspaceMessage
"""

_log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .campaign_orchestrator import CampaignOrchestrator


class CoreServiceIntersectClient:
    """
    This class handles interactions with INTERSECT and also helps out with broadcasting websocket events to connected clients.

    We should NOT use the main IntersectClient or IntersectService class here. As a core service, we interact with INTERSECT
    in a different way than generic services/clients would, and not in a way we should expose in the intersect_sdk library.
    """

    def __init__(self, settings: Settings) -> None:
        self.http_connections: set[Queue[bytes]] = set()
        self.campaign_orchestrator: CampaignOrchestrator | None = None
        """
        self.message_validator: TypeAdapter[EventMessage | LifecycleMessage | UserspaceMessage] = (
            TypeAdapter(EventMessage | LifecycleMessage | UserspaceMessage)
        )
        """

        if settings.BROKER_PROTOCOL == 'mqtt5.0':
            msg = 'MQTT 5.0 is currently not supported in the control plane fork.'
            raise RuntimeError(msg)

        self.control_plane_manager = ControlPlaneManager(
            control_configs=[
                ControlPlaneConfig(
                    protocol=settings.BROKER_PROTOCOL,
                    username=settings.BROKER_USERNAME,
                    password=settings.BROKER_PASSWORD,
                    port=settings.BROKER_PORT,
                    host=settings.BROKER_HOST,
                )
            ]
        )

        self.control_plane_manager.connect()
        self.control_plane_manager.add_subscription_channel(
            'test-topic', {self._handle_message}, True
        )

    def _handle_message(self, message: bytes, content_type: str, headers: dict[str, str]) -> None:
        """Broker callback calls this function when we get a message."""
        """
        try:
            # TODO - will need to be revisited when https://github.com/INTERSECT-SDK/python-sdk/issues/8 is fixed
            self.message_validator.validate_json(message)
        except ValidationError as e:
            _log.warning(e)
            _log.warning('non-INTERSECT message received: %s', message)
            return
        """

        # NOTE: for now, we will just blindly yield the message; it will always be valid JSON for now
        # This will not always be the case in the future - metadata will always be JSON structured, but raw data may be UTF-8
        # (this will eventually be managed by checking the message's Content-Type)

        if self.campaign_orchestrator is not None:
            self.campaign_orchestrator.handle_broker_message(message, content_type, headers)

        for connection in self.http_connections:
            connection.put_nowait(message)

    def set_campaign_orchestrator(self, orchestrator: CampaignOrchestrator) -> None:
        """Assign the campaign orchestrator for broker callbacks."""
        self.campaign_orchestrator = orchestrator

    def broadcast_message(self, message: bytes) -> None:
        """Sends a message to all Websocket clients."""
        for connection in self.http_connections:
            connection.put_nowait(message)

    def add_http_connection(self) -> Queue:
        """Add a Websocket subscriber."""
        queue: Queue[bytes] = Queue()
        self.http_connections.add(queue)

        return queue

    def remove_http_connection(self, queue: Queue) -> None:
        """Remove a Websocket subscriber."""
        try:
            self.http_connections.remove(queue)
        except KeyError:
            pass

    def is_connected(self) -> bool:
        """Check that we are still subscribed to the message broker."""
        return self.control_plane_manager.is_connected()

    def can_reconnect(self) -> bool:
        """Check that we can potentially resubscribe to the message broker."""
        return not self.control_plane_manager.considered_unrecoverable()

    def terminate(self) -> None:
        """Part of graceful shutdown."""
        for connection in self.http_connections:
            connection.put_nowait(b'')  # send empty message to indicate force-quit to WS clients
        self.control_plane_manager.disconnect()

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.icmp import Icmp
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator


@dataclass
class FakeNode:
    id: uuid.UUID
    metadata: dict[str, object]


class FakeControlPlaneManager:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, str, dict[str, str], bool]] = []

    def publish_message(
        self,
        channel: str,
        payload: bytes,
        content_type: str,
        headers: dict[str, str],
        persist: bool,
    ) -> None:
        self.published.append((channel, payload, content_type, headers, persist))


class FakeClient:
    def __init__(self) -> None:
        self.control_plane_manager = FakeControlPlaneManager()
        self.broadcasts: list[bytes] = []

    def broadcast_message(self, message: bytes) -> None:
        self.broadcasts.append(message)


def _event_types(broadcasts: list[bytes]) -> list[str]:
    return [json.loads(message.decode('utf-8'))['event']['event_type'] for message in broadcasts]


def _make_icmp(campaign_id: uuid.UUID, step_id: uuid.UUID) -> Icmp:
    metadata = {
        'campaignId': str(campaign_id),
    }
    step_metadata = {
        'topic': 'org/fac/system/subsystem/service/response',
        'headers': {
            'source': 'org.fac.system.subsystem.service',
            'sdk_version': '0.0.1',
        },
    }
    node = FakeNode(id=step_id, metadata=step_metadata)
    return Icmp.model_construct(nodes=[node], edges=[], metadata=metadata)


def test_handle_broker_message_completes_step() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    icmp = _make_icmp(campaign_id, step_id)

    orchestrator.submit_campaign(icmp)

    orchestrator.handle_broker_message(
        b'{}',
        'application/json',
        {
            'campaignId': str(campaign_id),
            'nodeId': str(step_id),
            'has_error': 'false',
            'source': 'org.fac.system.subsystem.service',
        },
    )

    assert _event_types(client.broadcasts) == [
        'STEP_START',
        'STEP_COMPLETE',
        'CAMPAIGN_COMPLETE',
    ]


def test_handle_broker_message_emits_error() -> None:
    client = FakeClient()
    orchestrator = CampaignOrchestrator(client)

    campaign_id = uuid.uuid4()
    step_id = uuid.uuid4()
    icmp = _make_icmp(campaign_id, step_id)

    orchestrator.submit_campaign(icmp)

    orchestrator.handle_broker_message(
        b'{}',
        'application/json',
        {
            'campaignId': str(campaign_id),
            'nodeId': str(step_id),
            'has_error': 'true',
            'source': 'org.fac.system.subsystem.service',
        },
    )

    assert _event_types(client.broadcasts)[-1] == 'CAMPAIGN_ERROR_FROM_SERVICE'

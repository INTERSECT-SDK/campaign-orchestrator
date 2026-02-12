from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..api.v1.endpoints.orchestrator.models.orchestrator_events import (
    CampaignCompleteEvent,
    CampaignErrorFromServiceEvent,
    OrchestratorEvent,
    StepCompleteEvent,
    StepStartEvent,
    UnknownErrorEvent,
)

if TYPE_CHECKING:
    from ..api.v1.endpoints.orchestrator.models.campaign import (
        Campaign,
        CampaignStepId,
        IntersectCampaignId,
    )
    from .intersect_client import CoreServiceIntersectClient


@dataclass
class CampaignState:
    campaign_id: IntersectCampaignId
    campaign_aliases: set[str]
    campaign: Campaign
    steps: list[CampaignStepId]
    current_index: int = 0
    active_step: CampaignStepId | None = None


class CampaignOrchestrator:
    """Track campaigns, execute steps, and react to broker callbacks."""

    def __init__(self, intersect_client: CoreServiceIntersectClient) -> None:
        self._client = intersect_client
        self._lock = threading.Lock()
        self._campaigns: dict[IntersectCampaignId, CampaignState] = {}
        self._campaign_aliases: dict[str, IntersectCampaignId] = {}

    def submit_campaign(self, campaign: Campaign) -> IntersectCampaignId:
        """Register a campaign and begin execution."""
        campaign_id = self._resolve_campaign_id(campaign)
        steps = self._steps_from_campaign(campaign)
        aliases = self._campaign_aliases_from_campaign(campaign)
        aliases.add(str(campaign_id))

        with self._lock:
            if campaign_id in self._campaigns:
                err_msg = f'Campaign already registered: {campaign_id}'
                raise ValueError(err_msg)
            state = CampaignState(
                campaign_id=campaign_id,
                campaign_aliases=aliases,
                campaign=campaign,
                steps=steps,
            )
            self._campaigns[campaign_id] = state
            for alias in aliases:
                self._campaign_aliases[alias] = campaign_id

        self._start_next_step(state)
        return campaign_id

    def cancel_campaign(self, campaign_id: IntersectCampaignId) -> bool:
        """Cancel a campaign and remove it from memory."""
        state = self._remove_campaign(campaign_id)
        if state is None:
            return False

        self._emit_event(
            campaign_id=state.campaign_id,
            event=UnknownErrorEvent(exception_message='Campaign cancelled by user'),
        )
        return True

    def handle_broker_message(
        self, message: bytes, content_type: str, headers: dict[str, str]
    ) -> None:
        """Process broker callbacks to advance campaign steps."""
        _ = content_type  # noqa: F841
        payload = self._parse_json(message)
        if payload is None:
            payload = {}

        campaign_id_raw = self._extract_campaign_id(headers, payload)
        if campaign_id_raw is None:
            return

        state = self._get_state_for_campaign_alias(campaign_id_raw)
        if state is None:
            return

        node_id = self._extract_node_id(headers, payload)
        if node_id is None or state.active_step is None:
            return

        if node_id != state.active_step:
            return

        has_error = self._has_error(headers)
        error_message = self._extract_error_message(has_error, payload)
        if error_message is not None:
            service_hierarchy = (
                self._extract_service_hierarchy(headers, payload) or 'unknown-service'
            )
            self._emit_event(
                campaign_id=state.campaign_id,
                event=CampaignErrorFromServiceEvent(
                    step_id=state.active_step,
                    service_hierarchy=service_hierarchy,
                    exception_message=error_message,
                ),
            )
            self._remove_campaign(state.campaign_id)
            return

        if not self._is_step_complete_message(has_error, payload):
            return

        self._complete_step(state, message)

    def _start_next_step(self, state: CampaignState) -> None:
        if state.current_index >= len(state.steps):
            self._finish_campaign(state)
            return

        state.active_step = state.steps[state.current_index]
        self._emit_event(
            campaign_id=state.campaign_id,
            event=StepStartEvent(step_id=state.active_step),
        )
        self._dispatch_step(state)

    def _complete_step(self, state: CampaignState, payload: bytes) -> None:
        if state.active_step is None:
            return

        self._emit_event(
            campaign_id=state.campaign_id,
            event=StepCompleteEvent(step_id=state.active_step, payload=payload),
        )

        state.current_index += 1
        state.active_step = None
        self._start_next_step(state)

    def _finish_campaign(self, state: CampaignState) -> None:
        self._emit_event(
            campaign_id=state.campaign_id,
            event=CampaignCompleteEvent(),
        )
        self._remove_campaign(state.campaign_id)

    def _emit_event(self, campaign_id: IntersectCampaignId, event: Any) -> None:
        orchestrator_event = OrchestratorEvent(campaign_id=campaign_id, event=event)
        self._client.broadcast_message(orchestrator_event.model_dump_json().encode('utf-8'))

    def _dispatch_step(self, state: CampaignState) -> None:
        if state.active_step is None:
            return
        try:
            step_metadata = self._step_metadata(state.campaign, state.active_step)
            headers = self._resolve_headers(step_metadata)
            topic = self._resolve_topic(step_metadata, headers)
            headers.setdefault('destination', topic)
            payload, content_type = self._resolve_payload(step_metadata)
        except ValueError as exc:
            self._emit_event(
                campaign_id=state.campaign_id,
                event=UnknownErrorEvent(exception_message=str(exc)),
            )
            self._remove_campaign(state.campaign_id)
            return

        self._client.control_plane_manager.publish_message(
            topic,
            payload,
            content_type,
            headers,
            persist=True,
        )

    def _remove_campaign(self, campaign_id: IntersectCampaignId) -> CampaignState | None:
        with self._lock:
            state = self._campaigns.pop(campaign_id, None)
            if state is None:
                return None
            for alias in state.campaign_aliases:
                self._campaign_aliases.pop(alias, None)
            return state

    def _get_state_for_campaign_alias(self, campaign_id_raw: str) -> CampaignState | None:
        with self._lock:
            campaign_id = self._campaign_aliases.get(campaign_id_raw)
            if campaign_id is None:
                return None
            return self._campaigns.get(campaign_id)

    def _step_metadata(self, campaign: Campaign, step_id: CampaignStepId) -> dict[str, Any]:
        # Try to find task metadata from the campaign's task_groups
        task = self._get_task_from_campaign(campaign, step_id)
        if task is not None:
            return self._task_to_metadata(task)

        # Fallback to campaign-level metadata if it exists
        return {
            'topic': 'org/fac/system/subsystem/service/response',
            'headers': {
                'source': 'org.fac.system.subsystem.service',
                'sdk_version': '0.0.1',
            },
        }

    def _get_task_from_campaign(
        self, campaign: Campaign, step_id: CampaignStepId
    ) -> dict[str, Any] | None:
        """Find a task in the campaign by its ID."""
        for task_group in campaign.task_groups:
            for task in task_group.tasks:
                try:
                    task_id = uuid.UUID(task.id)
                    if task_id == step_id:
                        return task.model_dump()
                except (TypeError, ValueError):
                    if task.id == str(step_id):
                        return task.model_dump()
        return None

    def _task_to_metadata(self, task: dict[str, Any]) -> dict[str, Any]:
        """Convert a task to metadata that can be used for dispatch."""
        hierarchy = task.get('hierarchy', 'org.fac.system.subsystem.service')
        return {
            'topic': f'{hierarchy.replace(".", "/")}/response',
            'headers': {
                'source': hierarchy,
                'sdk_version': '0.0.1',
            },
        }

    def _resolve_campaign_id(self, campaign: Campaign) -> IntersectCampaignId:
        """Resolve campaign ID from the campaign model."""
        try:
            return uuid.UUID(campaign.id)
        except (TypeError, ValueError):
            return uuid.uuid4()

    def _campaign_aliases_from_campaign(self, campaign: Campaign) -> set[str]:
        """Get campaign aliases from the campaign model."""
        aliases: set[str] = set()
        aliases.add(campaign.id)
        return aliases

    def _steps_from_campaign(self, campaign: Campaign) -> list[CampaignStepId]:
        """Extract step IDs from the campaign's task_groups."""
        steps: list[CampaignStepId] = []
        for task_group in campaign.task_groups:
            for task in task_group.tasks:
                try:
                    step_id = uuid.UUID(task.id)
                    steps.append(step_id)
                except (TypeError, ValueError):
                    continue
        return steps

    def _parse_json(self, message: bytes) -> dict[str, Any] | None:
        try:
            return json.loads(message)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None

    def _extract_campaign_id(self, headers: dict[str, str], payload: dict[str, Any]) -> str | None:
        for key in ('campaignId', 'campaign_id', 'id'):
            value = headers.get(key)
            if isinstance(value, str):
                return value

        for header in self._candidate_headers(payload):
            value = header.get('campaignId')
            if isinstance(value, str):
                return value
        value = payload.get('campaignId')
        if isinstance(value, str):
            return value
        return None

    def _extract_node_id(
        self, headers: dict[str, str], payload: dict[str, Any]
    ) -> CampaignStepId | None:
        for key in ('nodeId', 'node_id'):
            value = headers.get(key)
            node_id = self._normalize_node_id(value)
            if node_id is not None:
                return node_id

        for header in self._candidate_headers(payload):
            value = header.get('nodeId')
            node_id = self._normalize_node_id(value)
            if node_id is not None:
                return node_id
        value = payload.get('nodeId')
        return self._normalize_node_id(value)

    def _normalize_node_id(self, value: Any) -> CampaignStepId | None:
        if isinstance(value, list):
            if not value:
                return None
            value = value[0]
        if value is None:
            return None
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None

    def _candidate_headers(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        headers: list[dict[str, Any]] = []
        for key in ('header', 'headers', 'parent_header'):
            value = payload.get(key)
            if isinstance(value, dict):
                headers.append(value)
        return headers

    def _is_step_complete_message(self, has_error: bool | None, payload: dict[str, Any]) -> bool:
        if has_error is not None:
            return not has_error

        for header in self._candidate_headers(payload):
            header_error = header.get('has_error')
            if isinstance(header_error, bool):
                return not header_error
        return False

    def _extract_error_message(self, has_error: bool | None, payload: dict[str, Any]) -> str | None:
        if has_error is not True:
            return None
        error_payload = payload.get('payload') or payload.get('content') or payload
        return str(error_payload)

    def _extract_service_hierarchy(
        self, headers: dict[str, str], payload: dict[str, Any]
    ) -> str | None:
        source = headers.get('source')
        if isinstance(source, str):
            return source
        for header in self._candidate_headers(payload):
            header_source = header.get('source')
            if isinstance(header_source, str):
                return header_source
        return None

    def _has_error(self, headers: dict[str, str]) -> bool | None:
        value = headers.get('has_error')
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ('true', '1', 'yes'):
                return True
            if normalized in ('false', '0', 'no'):
                return False
        return None

    def _resolve_headers(self, metadata: dict[str, Any]) -> dict[str, str]:
        headers: dict[str, Any] = {}
        metadata_headers = metadata.get('headers') or metadata.get('header')
        if isinstance(metadata_headers, dict):
            headers.update(metadata_headers)

        for key in (
            'source',
            'destination',
            'created_at',
            'sdk_version',
            'data_handler',
            'has_error',
            'campaignId',
            'nodeId',
        ):
            value = metadata.get(key)
            if value is not None and key not in headers:
                headers[key] = value

        headers.setdefault('created_at', datetime.now(UTC).isoformat())
        headers.setdefault('has_error', False)

        required = {'source', 'sdk_version'}
        missing = sorted(key for key in required if key not in headers)
        if missing:
            err_msg = f'Missing required headers for step: {", ".join(missing)}'
            raise ValueError(err_msg)
        return {key: self._normalize_header_value(value) for key, value in headers.items()}

    def _resolve_topic(self, metadata: dict[str, Any], headers: dict[str, Any]) -> str:
        topic_value = metadata.get('topic')
        if isinstance(topic_value, str) and topic_value:
            return topic_value

        hierarchy_value = metadata.get('service_hierarchy') or metadata.get('source')
        if not isinstance(hierarchy_value, str) or not hierarchy_value:
            hierarchy_value = (
                headers.get('source') if isinstance(headers.get('source'), str) else None
            )

        hierarchy_parts = self._split_hierarchy(hierarchy_value)
        if hierarchy_parts:
            return '/'.join([*hierarchy_parts, 'response'])

        parts = []
        for key in ('organization', 'facility', 'system', 'subsystem', 'service'):
            value = metadata.get(key)
            if not isinstance(value, str) or not value:
                break
            parts.append(value)
        if len(parts) == 5:
            return '/'.join([*parts, 'response'])

        err_msg = 'Unable to resolve broker topic for campaign step'
        raise ValueError(err_msg)

    def _split_hierarchy(self, value: str | None) -> list[str]:
        if not value:
            return []
        if '/' in value:
            parts = [part for part in value.split('/') if part]
        else:
            parts = [part for part in value.split('.') if part]
        if len(parts) >= 5:
            return parts[:5]
        return []

    def _resolve_payload(self, metadata: dict[str, Any]) -> tuple[bytes, str]:
        content_type = self._resolve_content_type(metadata)
        raw = None
        for key in ('payload', 'input', 'data'):
            if key in metadata:
                raw = metadata.get(key)
                break

        if raw is None:
            return b'', content_type

        if isinstance(raw, bytes):
            # TODO: confirm how raw binary payloads should be represented on the broker.
            return raw, content_type

        if isinstance(raw, str):
            return raw.encode('utf-8'), content_type

        if content_type == 'application/octet-stream':
            content_type = 'application/json'
        return json.dumps(raw).encode('utf-8'), content_type

    def _resolve_content_type(self, metadata: dict[str, Any]) -> str:
        for key in ('content_type', 'contentType'):
            value = metadata.get(key)
            if isinstance(value, str) and value:
                return value
        return 'application/octet-stream'

    def _normalize_header_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return 'true' if value else 'false'
        return str(value)

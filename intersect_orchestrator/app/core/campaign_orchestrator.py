from __future__ import annotations

import json
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from snakes import ConstraintError
from snakes.nets import PetriNet

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
    from ..api.v1.endpoints.orchestrator.models.campaign_state import CampaignState as CampaignStateModel
    from .intersect_client import CoreServiceIntersectClient

from ..api.v1.endpoints.orchestrator.models.campaign_state import (
    CampaignState as CampaignStateModel,
    ExecutionStatus,
)
from ..converters.campaign_to_petri_net import CampaignPetriNetConverter
from .repository import CampaignEvent, CampaignRepository, InMemoryCampaignRepository


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

    def __init__(
        self,
        intersect_client: CoreServiceIntersectClient,
        repository: CampaignRepository | None = None,
    ) -> None:
        self._client = intersect_client
        self._lock = threading.Lock()
        self._campaigns: dict[IntersectCampaignId, CampaignState] = {}
        self._campaign_aliases: dict[str, IntersectCampaignId] = {}
        self._campaign_petri_nets: dict[IntersectCampaignId, PetriNet] = {}
        self._repository = repository or InMemoryCampaignRepository()

    def submit_campaign(self, campaign: Campaign) -> IntersectCampaignId:
        """Register a campaign and begin execution."""
        campaign_id = self._resolve_campaign_id(campaign)
        steps = self._steps_from_campaign(campaign)
        aliases = self._campaign_aliases_from_campaign(campaign)
        aliases.add(str(campaign_id))
        campaign_state = CampaignStateModel.from_campaign(
            campaign,
            status=ExecutionStatus.QUEUED,
        )
        petri_net = CampaignPetriNetConverter().convert(campaign_state)

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
            self._campaign_petri_nets[campaign_id] = petri_net

        self._repository.create_campaign(campaign_id, campaign, campaign_state)

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
        self._record_campaign_event(
            campaign_id=state.campaign_id,
            event_type='CAMPAIGN_CANCELLED',
            payload={'reason': 'Campaign cancelled by user'},
        )
        return True

    def get_campaign(self, campaign_id: IntersectCampaignId) -> Campaign | None:
        """Get a campaign payload from memory."""
        return self._repository.get_campaign(campaign_id)

    def get_campaign_state(self, campaign_id: IntersectCampaignId) -> CampaignStateModel | None:
        """Get a campaign state snapshot from memory."""
        snapshot = self._repository.load_snapshot(campaign_id)
        if snapshot is None:
            return None
        return snapshot.state

    def get_campaign_petri_net(self, campaign_id: IntersectCampaignId) -> PetriNet | None:
        """Get the Petri Net for a campaign from memory."""
        with self._lock:
            return self._campaign_petri_nets.get(campaign_id)

    def fire_petri_transition(self, campaign_id: IntersectCampaignId, transition_name: str) -> None:
        """Fire a Petri Net transition and update campaign state."""
        petri_net = self.get_campaign_petri_net(campaign_id)
        if petri_net is None:
            msg = f'Petri Net not found for campaign: {campaign_id}'
            raise ValueError(msg)

        try:
            transition = petri_net.transition(transition_name)
            if not transition.enabled(petri_net):
                msg = f"Transition '{transition_name}' is not enabled"
                raise ValueError(msg)
            transition.fire(petri_net)
        except (KeyError, ConstraintError) as err:
            msg = f"Transition '{transition_name}' does not exist in the Petri Net"
            raise ValueError(msg) from err

        self._handle_petri_transition(campaign_id, transition_name)

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
            self._record_campaign_event(
                campaign_id=state.campaign_id,
                event_type='CAMPAIGN_ERROR',
                payload={'error': error_message, 'step_id': str(state.active_step)},
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

        self._record_campaign_event(
            campaign_id=state.campaign_id,
            event_type='CAMPAIGN_STARTED',
            payload={'step_id': str(state.steps[state.current_index])},
        )
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
        self._record_event(
            campaign_id=state.campaign_id,
            event_type='STEP_COMPLETE',
            payload={'step_id': str(state.active_step)},
        )

        state.current_index += 1
        state.active_step = None
        self._start_next_step(state)

    def _finish_campaign(self, state: CampaignState) -> None:
        self._emit_event(
            campaign_id=state.campaign_id,
            event=CampaignCompleteEvent(),
        )
        self._record_campaign_event(
            campaign_id=state.campaign_id,
            event_type='CAMPAIGN_COMPLETED',
            payload={},
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
            self._record_campaign_event(
                campaign_id=state.campaign_id,
                event_type='CAMPAIGN_ERROR',
                payload={'error': str(exc)},
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

    def _record_event(
        self, campaign_id: IntersectCampaignId, event_type: str, payload: dict[str, Any]
    ) -> None:
        snapshot = self._repository.load_snapshot(campaign_id)
        if snapshot is None:
            return
        event = CampaignEvent(
            event_id=uuid.uuid4(),
            campaign_id=campaign_id,
            seq=snapshot.version + 1,
            event_type=event_type,
            payload=payload,
            timestamp=datetime.now(UTC),
        )
        self._repository.append_event(event, expected_version=snapshot.version)

    def _record_task_event(
        self,
        campaign_id: IntersectCampaignId,
        task_group_id: str,
        task_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Record a task-level event and update task state.
        
        Task event types: TASK_NOT_RUNNING, TASK_RUNNING, TASK_COMPLETED, TASK_FAILED, TASK_EVENT_RECEIVED
        """
        snapshot = self._repository.load_snapshot(campaign_id)
        if snapshot is None:
            return

        # Add hierarchy info to payload
        enriched_payload = {
            'task_group_id': task_group_id,
            'task_id': task_id,
            **payload,
        }

        # Create and append event
        event = CampaignEvent(
            event_id=uuid.uuid4(),
            campaign_id=campaign_id,
            seq=snapshot.version + 1,
            event_type=event_type,
            payload=enriched_payload,
            timestamp=datetime.now(UTC),
        )
        self._repository.append_event(event, expected_version=snapshot.version)

        # Update task state based on event type
        status_map = {
            'TASK_NOT_RUNNING': ExecutionStatus.QUEUED,
            'TASK_RUNNING': ExecutionStatus.RUNNING,
            'TASK_COMPLETED': ExecutionStatus.COMPLETE,
            'TASK_FAILED': ExecutionStatus.ERROR,
            # TASK_EVENT_RECEIVED doesn't change status - task is still running
        }

        if event_type in status_map:
            # Find and update task status in snapshot
            for task_group in snapshot.state.task_groups:
                if task_group.id == task_group_id:
                    for task in task_group.tasks:
                        if task.id == task_id:
                            task.status = status_map[event_type]
                            break
                    break

            # Update snapshot
            snapshot.version = event.seq
            snapshot.updated_at = event.timestamp
            self._repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    def _record_task_group_event(
        self,
        campaign_id: IntersectCampaignId,
        task_group_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Record a task group-level event and update task group state.
        
        Task group event types: TASK_GROUP_STARTED, TASK_GROUP_COMPLETED, TASK_GROUP_OBJECTIVE_MET
        """
        snapshot = self._repository.load_snapshot(campaign_id)
        if snapshot is None:
            return

        # Add hierarchy info to payload
        enriched_payload = {
            'task_group_id': task_group_id,
            **payload,
        }

        # Create and append event
        event = CampaignEvent(
            event_id=uuid.uuid4(),
            campaign_id=campaign_id,
            seq=snapshot.version + 1,
            event_type=event_type,
            payload=enriched_payload,
            timestamp=datetime.now(UTC),
        )
        self._repository.append_event(event, expected_version=snapshot.version)

        # Update task group state based on event type
        status_map = {
            'TASK_GROUP_STARTED': ExecutionStatus.RUNNING,
            'TASK_GROUP_COMPLETED': ExecutionStatus.COMPLETE,
        }

        if event_type in status_map:
            # Find and update task group status in snapshot
            for task_group in snapshot.state.task_groups:
                if task_group.id == task_group_id:
                    task_group.status = status_map[event_type]
                    break

            # Update snapshot
            snapshot.version = event.seq
            snapshot.updated_at = event.timestamp
            self._repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    def _record_task_group_objective_met(
        self,
        campaign_id: IntersectCampaignId,
        task_group_id: str,
        objective_id: str,
    ) -> None:
        """Record when a task group objective is met.
        
        Fires TWO events: TASK_GROUP_OBJECTIVE_MET followed by TASK_GROUP_COMPLETED.
        """
        # First: record objective met event
        self._record_task_group_event(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            event_type='TASK_GROUP_OBJECTIVE_MET',
            payload={'objective_id': objective_id},
        )

        # Second: record task group completed event
        self._record_task_group_event(
            campaign_id=campaign_id,
            task_group_id=task_group_id,
            event_type='TASK_GROUP_COMPLETED',
            payload={'reason': 'objective_met', 'objective_id': objective_id},
        )

    def _record_campaign_event(
        self,
        campaign_id: IntersectCampaignId,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Record a campaign-level event and update campaign state.
        
        Campaign event types: CAMPAIGN_STARTED, CAMPAIGN_COMPLETED, CAMPAIGN_OBJECTIVE_MET, CAMPAIGN_CANCELLED
        """
        snapshot = self._repository.load_snapshot(campaign_id)
        if snapshot is None:
            return

        # Create and append event
        event = CampaignEvent(
            event_id=uuid.uuid4(),
            campaign_id=campaign_id,
            seq=snapshot.version + 1,
            event_type=event_type,
            payload=payload,
            timestamp=datetime.now(UTC),
        )
        self._repository.append_event(event, expected_version=snapshot.version)

        # Update campaign state based on event type
        status_map = {
            'CAMPAIGN_STARTED': ExecutionStatus.RUNNING,
            'CAMPAIGN_COMPLETED': ExecutionStatus.COMPLETE,
            'CAMPAIGN_CANCELLED': ExecutionStatus.ERROR,
        }

        if event_type in status_map:
            snapshot.state.status = status_map[event_type]

            # Update snapshot
            snapshot.version = event.seq
            snapshot.updated_at = event.timestamp
            self._repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    def _handle_petri_transition(
        self, campaign_id: IntersectCampaignId, transition_name: str
    ) -> None:
        """Map Petri Net transitions to campaign state updates."""
        snapshot = self._repository.load_snapshot(campaign_id)
        if snapshot is None:
            return

        if transition_name == 'finalize_campaign':
            self._record_campaign_event(
                campaign_id=campaign_id,
                event_type='CAMPAIGN_COMPLETED',
                payload={'transition': transition_name},
            )
            return

        for task_group in snapshot.state.task_groups:
            if transition_name == f'activate_{task_group.id}':
                self._record_task_group_event(
                    campaign_id=campaign_id,
                    task_group_id=task_group.id,
                    event_type='TASK_GROUP_STARTED',
                    payload={'transition': transition_name},
                )
                return

            if transition_name == f'complete_{task_group.id}':
                self._record_task_group_event(
                    campaign_id=campaign_id,
                    task_group_id=task_group.id,
                    event_type='TASK_GROUP_COMPLETED',
                    payload={'transition': transition_name},
                )
                return

            for task in task_group.tasks:
                if transition_name == f'task_{task_group.id}_{task.id}':
                    self._record_task_event(
                        campaign_id=campaign_id,
                        task_group_id=task_group.id,
                        task_id=task.id,
                        event_type='TASK_COMPLETED',
                        payload={'transition': transition_name},
                    )
                    return

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

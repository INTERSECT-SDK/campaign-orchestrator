from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from intersect_sdk_common import IntersectDataHandler
from intersect_sdk_common.control_plane.messages.event import (
    validate_event_message_headers,
)
from intersect_sdk_common.control_plane.messages.userspace import (
    create_userspace_message_headers,
    validate_userspace_message_headers,
)
from pydantic import ValidationError
from snakes import ConstraintError

from ..api.v1.endpoints.orchestrator.models.orchestrator_events import (
    CampaignCompleteEvent,
    CampaignErrorFromServiceEvent,
    OrchestratorEvent,
    OrchestratorEventType,
    StepCompleteEvent,
    StepStartEvent,
    UnknownErrorEvent,
)

if TYPE_CHECKING:
    from snakes.nets import PetriNet

    from ..api.v1.endpoints.orchestrator.models.campaign import (
        Campaign,
        CampaignStepId,
        IntersectCampaignId,
        Task,
    )
    from ..api.v1.endpoints.orchestrator.models.campaign_state import (
        CampaignState as CampaignStateModel,
    )
    from .intersect_client import CoreServiceIntersectClient

from ..api.v1.endpoints.orchestrator.models.campaign import (
    ObjectiveAssert,
    ObjectiveIterate,
)
from ..api.v1.endpoints.orchestrator.models.campaign_state import (
    CampaignInfo,
    ExecutionStatus,
)
from ..api.v1.endpoints.orchestrator.models.campaign_state import (
    CampaignState as CampaignStateModel,
)
from ..converters.campaign_to_petri_net import CampaignPetriNetConverter
from .objective_checkers import AssertChecker, IterateChecker, ObjectiveChecker
from .repository import (
    CampaignEvent,
    CampaignRepository,
    InMemoryCampaignRepository,
)

logger = logging.getLogger(__name__)


def _build_checkers(
    objectives: list[ObjectiveAssert | ObjectiveIterate],
) -> list[ObjectiveChecker]:
    """Turn the list of objective models on a task group into checker instances."""
    checkers: list[ObjectiveChecker] = []
    for obj in objectives:
        if isinstance(obj, ObjectiveIterate):
            checkers.append(IterateChecker(obj))
        elif isinstance(obj, ObjectiveAssert):
            checkers.append(AssertChecker(obj))
    return checkers


@dataclass
class TaskGroupExecution:
    """Tracks the execution state of a single task group.

    Iteration continues until *all* attached objective checkers report
    ``is_met() == True``.  If there are no objectives, the group runs
    exactly once (single-pass).
    """

    task_group_id: uuid.UUID
    task_ids: list[uuid.UUID]
    objective_checkers: list[ObjectiveChecker] = field(default_factory=list)
    current_iteration: int = 0
    active_tasks: set[uuid.UUID] = field(default_factory=set)
    pending_tasks: set[uuid.UUID] = field(default_factory=set)
    """Tasks whose task_dependencies have not yet all completed; not yet dispatched."""
    event_listener_tasks: set[uuid.UUID] = field(default_factory=set)
    """Tasks that have been subscribed for event delivery in the current group."""
    completed_tasks: set[uuid.UUID] = field(default_factory=set)
    iteration_payloads: dict[uuid.UUID, bytes] = field(default_factory=dict)

    def objectives_met(self) -> bool:
        """Return *True* when every objective checker is satisfied.

        When there are no objectives the group is "met" after the first
        completed iteration (``current_iteration >= 1``).
        """
        if not self.objective_checkers:
            return self.current_iteration >= 1
        return all(c.is_met() for c in self.objective_checkers)


@dataclass
class CampaignState:
    campaign_run_id: uuid.UUID
    """the run_id from the Campaign object submitted by clients"""
    campaign: Campaign
    task_group_executions: list[TaskGroupExecution]
    current_group_index: int = 0
    resolved_output_values: dict[uuid.UUID, Any] = field(default_factory=dict)
    """Maps output Value IDs from completed tasks to their resolved runtime values.
    Persists across task groups so upstream outputs flow into downstream inputs."""
    status: ExecutionStatus = ExecutionStatus.RUNNING
    """Current execution status of the campaign."""
    lock: threading.Lock = field(default_factory=threading.Lock)
    """Per-campaign lock to serialise broker callback threads."""


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
        self._campaign_petri_nets: dict[IntersectCampaignId, PetriNet] = {}
        self._repository = repository or InMemoryCampaignRepository()

    def submit_campaign(self, campaign: Campaign) -> IntersectCampaignId:
        """Register a campaign and begin execution."""
        campaign_id = campaign.run_id
        executions = self._build_task_group_executions(campaign)

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
                campaign_run_id=campaign_id,
                campaign=campaign,
                task_group_executions=executions,
            )
            self._campaigns[campaign_id] = state
            self._campaign_petri_nets[campaign_id] = petri_net

        self._repository.create_campaign(campaign_id, campaign, campaign_state)

        # Hold the per-campaign lock during the initial dispatch so that
        # a fast service reply arriving in a broker callback thread cannot race with
        # the first iteration setup.
        with state.lock:
            self._start_next_iteration(state)
        return campaign_id

    def cancel_campaign(self, campaign_id: IntersectCampaignId) -> bool:
        """Cancel a campaign and remove it from memory."""
        state = self._remove_campaign(campaign_id)
        if state is None:
            return False

        self._emit_event(
            campaign_id=state.campaign.id,
            run_id=state.campaign_run_id,
            event=UnknownErrorEvent(exception_message='Campaign cancelled by user'),
        )
        self._record_campaign_event(
            campaign_id=state.campaign_run_id,
            event_type='CAMPAIGN_CANCELLED',
            payload={'reason': 'Campaign cancelled by user'},
        )
        return True

    def list_campaigns(self, status: list[ExecutionStatus] | None = None) -> list[CampaignInfo]:
        """List campaigns, optionally filtered by status.

        Args:
            status: If provided, filter to only campaigns with this status.
                   If None, return all campaigns (running only for now).

        Returns:
            List of CampaignInfo objects with campaign_id and status.
        """
        with self._lock:
            # Currently we only track running campaigns in memory
            campaigns = []
            for _campaign_id, state in self._campaigns.items():
                # Filter by status if provided
                if status is not None and state.status not in status:
                    continue
                campaigns.append(
                    CampaignInfo(
                        campaign_id=str(state.campaign.id),
                        status=state.status,
                    )
                )
            return campaigns

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

    def handle_request_reply_broker_message(
        self, message: bytes, content_type: str, raw_headers: dict[str, str]
    ) -> None:
        """Process broker callbacks from Reply messages to advance campaign steps. This is the "entrypoint" from the INTERSECT ControlPlane.

        This does not apply to Event messages, use another callback for this.
        """
        # FIXME - several of the fast-return error cases should emit events and CANCEL the campaign
        try:
            headers = validate_userspace_message_headers(raw_headers)
        except ValidationError as e:
            logger.warning('Invalid message headers, rejecting: %s', str(e))
            # abort, but attempt cleanup before aborting
            campaign_id_str = raw_headers.get('campaign_id')
            if campaign_id_str:
                campaign_id = uuid.UUID(campaign_id_str)
                state = self._get_state_for_campaign_alias(campaign_id)
                if state:
                    failed_step_id = None
                    request_id = raw_headers.get('request_id')
                    if request_id:
                        try:
                            failed_step_id = uuid.UUID(request_id)
                        except ValueError:
                            failed_step_id = None
                    self._handle_request_reply_service_error(
                        state=state,
                        source=raw_headers.get('source', '???'),
                        error_message=f'Invalid message headers: {e}',
                        failed_step_id=failed_step_id,
                    )
            return

        campaign_id = headers.campaign_id

        state = self._get_state_for_campaign_alias(campaign_id)
        if state is None:
            # will be unable to cleanup
            logger.error('Could not find state for campaign ID: %s', campaign_id)
            return

        # from here, should be able to always cleanup

        node_id = headers.request_id

        # Acquire per-campaign lock to serialise concurrent broker reply threads.
        with state.lock:
            # Check that the reply matches one of the currently active tasks
            if state.current_group_index >= len(state.task_group_executions):
                logger.error('Campaign has no active task group for ID: %s', campaign_id)
                self._handle_request_reply_service_error(
                    state,
                    headers.source,
                    'No active task group',
                    failed_step_id=node_id,
                )
                return

            execution = state.task_group_executions[state.current_group_index]
            if node_id not in execution.active_tasks:
                logger.error(
                    'Node ID %s from message is not among active tasks for campaign ID: %s',
                    node_id,
                    campaign_id,
                )
                self._handle_request_reply_service_error(
                    state,
                    headers.source,
                    'Node ID from message does not match any active task',
                    failed_step_id=node_id,
                )
                return

            if content_type == 'application/json':
                try:
                    payload = json.loads(message)
                except json.JSONDecodeError:
                    logger.exception('Message claimed to be JSON but was not')
                    self._handle_request_reply_service_error(
                        state,
                        headers.source,
                        'Message claimed to be JSON but was not',
                        failed_step_id=node_id,
                    )
                    return
            else:
                payload = message

            if headers.has_error:
                self._handle_request_reply_service_error(
                    state,
                    headers.source,
                    str(payload),
                    failed_step_id=node_id,
                )
                return

            self._complete_step(state, node_id, message)

    def handle_event_broker_message(
        self, message: bytes, _content_type: str, raw_headers: dict[str, str]
    ) -> None:
        """Process broker callbacks from Event messages to advance event tasks."""
        try:
            headers = validate_event_message_headers(raw_headers)
        except ValidationError as err:
            logger.warning('Invalid event headers, ignoring event: %s', str(err))
            return

        logger.info(
            'received event message source=%s capability=%s event_name=%s',
            headers.source,
            headers.capability_name,
            headers.event_name,
        )

        matching_steps: list[tuple[CampaignState, uuid.UUID]] = []

        with self._lock:
            active_states = list(self._campaigns.values())

        for state in active_states:
            if state.current_group_index >= len(state.task_group_executions):
                continue

            execution = state.task_group_executions[state.current_group_index]
            if not execution.event_listener_tasks:
                continue

            for task_id in list(execution.event_listener_tasks):
                task = self._get_task_from_campaign(state.campaign, task_id)
                if task is None or task.event_name is None:
                    continue

                if (
                    task.hierarchy == headers.source
                    and task.capability == headers.capability_name
                    and task.event_name == headers.event_name
                ):
                    matching_steps.append((state, task_id))

        if not matching_steps:
            logger.info(
                'No active event task matched source=%s capability=%s event_name=%s',
                headers.source,
                headers.capability_name,
                headers.event_name,
            )
            return

        logger.info(
            'Matched event message source=%s capability=%s event_name=%s to %d task(s)',
            headers.source,
            headers.capability_name,
            headers.event_name,
            len(matching_steps),
        )

        for state, task_id in matching_steps:
            current_state = self._get_state_for_campaign_alias(state.campaign_run_id)
            if current_state is None:
                continue

            # Acquire per-campaign lock to serialise concurrent broker callback threads:
            # without it two threads can both see the same event task in
            # active_tasks and both call _complete_step, causing double-dispatch
            # of dependent tasks (and the second reply killing the campaign).
            with current_state.lock:
                if current_state.current_group_index >= len(current_state.task_group_executions):
                    continue

                execution = current_state.task_group_executions[current_state.current_group_index]
                if task_id not in execution.active_tasks:
                    continue

                self._record_task_event(
                    campaign_id=current_state.campaign_run_id,
                    task_group_id=execution.task_group_id,
                    task_id=task_id,
                    event_type='TASK_EVENT_RECEIVED',
                    payload={
                        'source': headers.source,
                        'capability_name': headers.capability_name,
                        'event_name': headers.event_name,
                    },
                )
                self._complete_step(current_state, task_id, message)

    def _handle_request_reply_service_error(
        self,
        state: CampaignState,
        source: str,
        error_message: str,
        failed_step_id: uuid.UUID | None = None,
    ) -> None:
        """Handle error responses from services.

        Assumes that the Service messed up, that we have a campaign ID for event hooks,
        and that we have a source in "org.fac.sys.subsys.svc" format.
        """
        # Report the first active task as the failed step for the event
        execution = (
            state.task_group_executions[state.current_group_index]
            if state.current_group_index < len(state.task_group_executions)
            else None
        )
        failed_step = failed_step_id
        if failed_step is None:
            failed_step = (
                next(iter(execution.active_tasks))
                if execution and execution.active_tasks
                else uuid.UUID(int=0)
            )

        self._emit_event(
            campaign_id=state.campaign.id,
            run_id=state.campaign_run_id,
            event=CampaignErrorFromServiceEvent(
                step_id=failed_step,
                service_hierarchy=source,
                exception_message=error_message,
            ),
        )
        self._record_campaign_event(
            campaign_id=state.campaign_run_id,
            event_type='CAMPAIGN_ERROR',
            payload={'error': error_message, 'step_id': str(failed_step)},
        )
        self._remove_campaign(state.campaign_run_id)

    def _start_next_iteration(self, state: CampaignState) -> None:
        """Start the next iteration of the current task group, or advance to the next group."""
        logger.debug('_start_next_iteration: group_index=%d', state.current_group_index)

        if state.current_group_index >= len(state.task_group_executions):
            logger.debug('all task groups complete, finishing campaign')
            self._finish_campaign(state)
            return

        execution = state.task_group_executions[state.current_group_index]

        if execution.objectives_met():
            # All objectives for this task group are satisfied — advance
            logger.debug(
                'task group %s objectives met after %d iterations, advancing',
                execution.task_group_id,
                execution.current_iteration,
            )
            if execution.objective_checkers:
                # Record objective-met events for each objective
                for checker in execution.objective_checkers:
                    self._record_task_group_event(
                        campaign_id=state.campaign_run_id,
                        task_group_id=execution.task_group_id,
                        event_type='TASK_GROUP_OBJECTIVE_MET',
                        payload={'objective_id': str(checker.objective_id)},
                    )
                # Record a single completion event now that all objectives are met
                self._record_task_group_event(
                    campaign_id=state.campaign_run_id,
                    task_group_id=execution.task_group_id,
                    event_type='TASK_GROUP_COMPLETED',
                    payload={'reason': 'objectives_met'},
                )
            else:
                # No objectives — just record task group completed
                self._record_task_group_event(
                    campaign_id=state.campaign_run_id,
                    task_group_id=execution.task_group_id,
                    event_type='TASK_GROUP_COMPLETED',
                    payload={'reason': 'single_pass'},
                )
            state.current_group_index += 1
            self._start_next_iteration(state)
            return

        if execution.current_iteration == 0 and state.current_group_index == 0:
            # First iteration of first group — emit CAMPAIGN_STARTED
            self._record_campaign_event(
                campaign_id=state.campaign_run_id,
                event_type='CAMPAIGN_STARTED',
                payload={'task_group_id': str(execution.task_group_id)},
            )

        if execution.current_iteration == 0:
            self._record_task_group_event(
                campaign_id=state.campaign_run_id,
                task_group_id=execution.task_group_id,
                event_type='TASK_GROUP_STARTED',
                payload={'iteration': 0},
            )

        logger.debug(
            'dispatching iteration %d for task group %s (%d tasks)',
            execution.current_iteration + 1,
            execution.task_group_id,
            len(execution.task_ids),
        )

        execution.completed_tasks = set()
        execution.iteration_payloads = {}

        # Separate tasks into immediately-ready (no unmet dependencies) vs pending.
        ready: set[uuid.UUID] = set()
        pending: set[uuid.UUID] = set()
        for task_id in execution.task_ids:
            task = self._get_task_from_campaign(state.campaign, task_id)
            if task is None:
                continue
            if not task.task_dependencies or all(
                dep in execution.completed_tasks for dep in task.task_dependencies
            ):
                ready.add(task_id)
            else:
                pending.add(task_id)

        execution.active_tasks = ready
        execution.pending_tasks = pending

        for task_id in ready:
            self._emit_event(
                campaign_id=state.campaign.id,
                run_id=state.campaign_run_id,
                event=StepStartEvent(step_id=task_id),
            )

        self._dispatch_active_tasks(state)

    def _complete_step(self, state: CampaignState, step_id: uuid.UUID, payload: bytes) -> None:
        """Mark a single task as complete. When all tasks in the current iteration
        are done, notify objective checkers and decide whether to iterate again."""
        execution = state.task_group_executions[state.current_group_index]
        task = self._get_task_from_campaign(state.campaign, step_id)
        if task is None:
            msg = f'No task found for step ID: {step_id} in campaign ID: {state.campaign_run_id}'
            logger.error(msg)
            self._handle_dispatch_error(state, msg)
            return

        execution.active_tasks.discard(step_id)
        execution.completed_tasks.add(step_id)
        execution.iteration_payloads[step_id] = payload

        self._emit_event(
            campaign_id=state.campaign.id,
            run_id=state.campaign_run_id,
            event=StepCompleteEvent(step_id=step_id, payload=payload),
        )
        self._record_event(
            campaign_id=state.campaign_run_id,
            event_type='STEP_COMPLETE',
            payload={'step_id': str(step_id)},
        )

        # Resolve and store output values from the completed task so downstream
        # tasks (in this or future groups) can reference them via shared value IDs.
        self._store_task_output_values(state, step_id, payload)

        if task.event_name is not None:
            newly_unblocked = self._pop_unblocked_tasks(state, execution)
            logger.info(
                'Event task %s completed for campaign %s; unblocked %d dependent task(s)',
                step_id,
                state.campaign_run_id,
                len(newly_unblocked),
            )
            if newly_unblocked:
                for task_id in newly_unblocked:
                    self._emit_event(
                        campaign_id=state.campaign.id,
                        run_id=state.campaign_run_id,
                        event=StepStartEvent(step_id=task_id),
                    )
                self._dispatch_task_ids(state, newly_unblocked)

            execution.completed_tasks.discard(step_id)
            execution.active_tasks.add(step_id)
            logger.info(
                'Reactivated event task %s for campaign %s; active_tasks=%d pending_tasks=%d',
                step_id,
                state.campaign_run_id,
                len(execution.active_tasks),
                len(execution.pending_tasks),
            )
            return

        # Unblock any pending tasks whose dependencies are now all satisfied.
        newly_unblocked = self._pop_unblocked_tasks(state, execution)
        if newly_unblocked:
            for task_id in newly_unblocked:
                self._emit_event(
                    campaign_id=state.campaign.id,
                    run_id=state.campaign_run_id,
                    event=StepStartEvent(step_id=task_id),
                )
            self._dispatch_task_ids(state, newly_unblocked)

        if not execution.active_tasks and not execution.pending_tasks:
            # All tasks in this iteration are done — let checkers observe
            for checker in execution.objective_checkers:
                checker.record_iteration(execution.iteration_payloads)

            execution.current_iteration += 1
            execution.active_tasks = set()
            execution.pending_tasks = set()
            execution.completed_tasks = set()
            execution.iteration_payloads = {}
            self._start_next_iteration(state)

    def _finish_campaign(self, state: CampaignState) -> None:
        self._emit_event(
            campaign_id=state.campaign.id,
            run_id=state.campaign_run_id,
            event=CampaignCompleteEvent(),
        )
        self._record_campaign_event(
            campaign_id=state.campaign_run_id,
            event_type='CAMPAIGN_COMPLETED',
            payload={},
        )
        self._remove_campaign(state.campaign_run_id)

    def _emit_event(
        self,
        campaign_id: IntersectCampaignId,
        run_id: IntersectCampaignId,
        event: OrchestratorEventType,
    ) -> None:
        logger.info(
            'emitting event: %s for campaign ID: %s',
            event.event_type,
            campaign_id,
        )
        logger.info('EVENT VALUE: %s', event)
        orchestrator_event = OrchestratorEvent(campaign_id=campaign_id, run_id=run_id, event=event)
        self._client.broadcast_message(orchestrator_event.model_dump_json().encode('utf-8'))

    def _dispatch_active_tasks(self, state: CampaignState) -> None:
        """Dispatch all currently active tasks in the task group."""
        execution = state.task_group_executions[state.current_group_index]
        self._dispatch_task_ids(state, list(execution.active_tasks))

    def _dispatch_task_ids(
        self, state: CampaignState, task_ids: set[uuid.UUID] | list[uuid.UUID]
    ) -> None:
        """Dispatch the given task IDs."""
        tasks: list[Task] = []

        for task_id in task_ids:
            task = self._get_task_from_campaign(state.campaign, task_id)
            if task is None:
                msg = (
                    f'No task found for step ID: {task_id} in campaign ID: {state.campaign_run_id}'
                )
                logger.error(msg)
                self._handle_dispatch_error(state, msg)
                return

            tasks.append(task)

        # Ensure event listeners are subscribed before request tasks publish messages.
        # This avoids dropping fast events emitted by request tasks in the same group.
        for task in tasks:
            if task.event_name is not None:
                self._dispatch_event(state, task)

        for task in tasks:
            # exactly one of operation_id or event_name should be defined, validation should occur on the model
            if task.operation_id is not None:
                self._dispatch_request(state, task)

    def _dispatch_request(self, state: CampaignState, task: Task) -> None:
        headers = create_userspace_message_headers(
            source=self._client.get_orchestrator_hierarchy(),
            destination=task.hierarchy,
            # make sure _dispatch_request is only called when task.operation_id has been validated to be truthy
            operation_id=f'{task.capability}.{task.operation_id}',
            # FIXME be more flexible about this later on
            data_handler=IntersectDataHandler.MESSAGE,
            campaign_id=state.campaign_run_id,
            request_id=task.id,
        )

        # The request payload is built from task input defaults in campaign JSON.
        # This allows static per-task configuration like stream IDs and seeds.
        #
        # FIXME hardcoding content-type and payload for now
        # some ideas on 'payload': this may be base-64 encoded for websocket purposes, but if so should be decoded before publishing message
        content_type = 'application/json'
        campaign_state = self._campaigns.get(state.campaign_run_id)
        resolved = campaign_state.resolved_output_values if campaign_state is not None else {}
        payload = self._build_task_request_payload(task, resolved)

        logger.debug('current campaigns: %s', list(self._campaigns.keys()))
        logger.debug(
            'PUBLISHING REQUEST MESSAGE to %s with headers %s',
            task.hierarchy,
            headers,
        )
        logger.info(
            'Dispatching request task %s to hierarchy=%s capability=%s operation=%s',
            task.id,
            task.hierarchy,
            task.capability,
            task.operation_id,
        )
        self._client.publish_request_message(
            task.hierarchy,
            payload,
            content_type,
            headers,
        )

    def _build_task_request_payload(
        self,
        task: Task,
        resolved_output_values: dict[uuid.UUID, Any] | None = None,
    ) -> bytes:
        """Build request payload for a task from input-schema defaults, overridden
        by any resolved output values from previously completed tasks.

        ``resolved_output_values`` maps output Value UUIDs from completed tasks to
        their runtime values.  When a task's input Value ID matches a key in that
        dict, the resolved value is used instead of the schema default.

        Supports two schema shapes:
        * Object schema (``{"type": "object", "properties": {...}}``): builds a
          dict payload with property values extracted from resolved values or defaults.
        * Primitive schema (``{"type": "string"}``, ``{"type": "integer"}``, etc.):
          sends the raw value directly (e.g., ``"uuid-value"`` instead of
          ``{"uuid": "uuid-value"}``). This is needed for capabilities that expect
          a raw primitive argument rather than a Pydantic model.
        """
        # TODO: Need to return b'null' for content type application/json
        #       and empty byte string for non-json content types
        if task.input is None:
            return b'null'

        if resolved_output_values is None:
            resolved_output_values = {}

        schema_type = task.input.json_schema.get('type', 'object')

        # Handle non-object schema types (string, integer, number, boolean, array)
        if schema_type != 'object':
            # For primitive types, use the single value's resolved value or schema default
            if task.input.values:
                value = task.input.values[0]
                if value.id in resolved_output_values:
                    raw_value = resolved_output_values[value.id]
                elif 'default' in task.input.json_schema:
                    raw_value = task.input.json_schema['default']
                else:
                    return b'null'
                return json.dumps(raw_value).encode('utf-8')
            if 'default' in task.input.json_schema:
                return json.dumps(task.input.json_schema['default']).encode('utf-8')
            return b'null'

        # Handle object schema type (existing logic)
        properties = task.input.json_schema.get('properties', {})
        payload: dict[str, Any] = {}

        for value in task.input.values:
            var_name = value.var
            # Resolved runtime value takes precedence over schema default.
            if value.id in resolved_output_values:
                payload[var_name] = resolved_output_values[value.id]
            else:
                property_schema = properties.get(var_name)
                if isinstance(property_schema, dict) and 'default' in property_schema:
                    payload[var_name] = property_schema['default']

        if not payload:
            return b''

        return json.dumps(payload).encode('utf-8')

    def _store_task_output_values(
        self, state: CampaignState, step_id: uuid.UUID, payload: bytes
    ) -> None:
        """Extract output values from a completed task's payload and store them
        in ``state.resolved_output_values`` keyed by their Value UUID.

        Supports two payload shapes:
        * JSON object: each ``var`` key in the output values is extracted from
          the decoded dict.
        * JSON scalar (string, number, bool): if the task declares exactly one
          output value, that scalar becomes the resolved value for its Value ID.
        """
        task = self._get_task_from_campaign(state.campaign, step_id)
        if task is None:
            return

        try:
            decoded = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            return

        if task.output is not None and task.output.values:
            if isinstance(decoded, dict):
                for val in task.output.values:
                    if val.var in decoded:
                        state.resolved_output_values[val.id] = decoded[val.var]
            elif len(task.output.values) == 1:
                # Scalar payload (e.g. a bare string or number) maps to the single output variable.
                state.resolved_output_values[task.output.values[0].id] = decoded
            return

        if task.event_name is None or task.input is None or not task.input.values:
            return

        if isinstance(decoded, dict):
            for val in task.input.values:
                if val.var in decoded:
                    state.resolved_output_values[val.id] = decoded[val.var]
        elif len(task.input.values) == 1:
            state.resolved_output_values[task.input.values[0].id] = decoded

    def _pop_unblocked_tasks(
        self, state: CampaignState, execution: TaskGroupExecution
    ) -> set[uuid.UUID]:
        """Return the subset of ``execution.pending_tasks`` whose task_dependencies
        are now all satisfied (present in ``execution.completed_tasks``), and
        remove them from ``pending_tasks`` / add them to ``active_tasks``."""
        newly_unblocked: set[uuid.UUID] = set()
        for task_id in list(execution.pending_tasks):
            task = self._get_task_from_campaign(state.campaign, task_id)
            if task is None:
                continue
            if all(dep in execution.completed_tasks for dep in task.task_dependencies):
                newly_unblocked.add(task_id)

        execution.pending_tasks -= newly_unblocked
        execution.active_tasks |= newly_unblocked
        return newly_unblocked

    def _dispatch_event(self, state: CampaignState, task: Task) -> None:
        # TODO - unsubscribe when no active campaigns depend on this service.
        if task.event_name is None:
            msg = f'Event task {task.id} missing event_name'
            self._handle_dispatch_error(state, msg)
            return

        try:
            logger.info(
                'Subscribing event task %s to hierarchy=%s capability=%s event_name=%s',
                task.id,
                task.hierarchy,
                task.capability,
                task.event_name,
            )
            self._client.subscribe_to_events(
                task.hierarchy,
                task.capability,
                task.event_name,
            )
            execution = state.task_group_executions[state.current_group_index]
            execution.event_listener_tasks.add(task.id)
        except Exception as err:
            msg = (
                f'Failed to subscribe to events for task {task.id} '
                f'(hierarchy={task.hierarchy}, capability={task.capability}, event_name={task.event_name}): {err}'
            )
            logger.exception(msg)
            self._handle_dispatch_error(state, msg)

    def _handle_dispatch_error(self, state: CampaignState, error_message: str) -> None:
        self._emit_event(
            campaign_id=state.campaign.id,
            run_id=state.campaign_run_id,
            event=UnknownErrorEvent(exception_message=error_message),
        )
        self._record_campaign_event(
            campaign_id=state.campaign_run_id,
            event_type='CAMPAIGN_ERROR',
            payload={'error': error_message},
        )
        self._remove_campaign(state.campaign_run_id)

    def _remove_campaign(self, campaign_id: IntersectCampaignId) -> CampaignState | None:
        with self._lock:
            state = self._campaigns.pop(campaign_id, None)
            if state is None:
                return None
            # Clean up Petri Net to avoid memory leaks
            self._campaign_petri_nets.pop(campaign_id, None)
            return state

    def _record_event(
        self,
        campaign_id: IntersectCampaignId,
        event_type: str,
        payload: dict[str, Any],
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

        # Even if this event doesn't change the in-memory state, we must advance
        # the snapshot version and updated_at to keep event sequencing consistent.
        snapshot.version = event.seq
        snapshot.updated_at = event.timestamp
        self._repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    def _record_task_event(
        self,
        campaign_id: IntersectCampaignId,
        task_group_id: uuid.UUID,
        task_id: uuid.UUID,
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

        # Always update snapshot version after appending event to ensure seq increments
        # for subsequent events (critical for events like TASK_EVENT_RECEIVED that don't change status)
        snapshot.version = event.seq
        snapshot.updated_at = event.timestamp

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

        # Update snapshot with version incremented
        self._repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    def _record_task_group_event(
        self,
        campaign_id: IntersectCampaignId,
        task_group_id: uuid.UUID,
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
            'task_group_id': str(task_group_id),
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

        # Always update snapshot version after appending event to ensure seq increments
        # for subsequent events (critical for multi-event sequences like objective_met -> completed)
        snapshot.version = event.seq
        snapshot.updated_at = event.timestamp

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

        # Update snapshot with version incremented
        self._repository.update_snapshot(snapshot, expected_version=event.seq - 1)

    def _record_task_group_objective_met(
        self,
        campaign_id: IntersectCampaignId,
        task_group_id: uuid.UUID,
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

        # Always update snapshot version after appending event to ensure seq increments
        # for subsequent events (critical for events like CAMPAIGN_OBJECTIVE_MET that don't change status)
        snapshot.version = event.seq
        snapshot.updated_at = event.timestamp

        # Update campaign state based on event type
        status_map = {
            'CAMPAIGN_STARTED': ExecutionStatus.RUNNING,
            'CAMPAIGN_COMPLETED': ExecutionStatus.COMPLETE,
            'CAMPAIGN_CANCELLED': ExecutionStatus.ERROR,
        }

        if event_type in status_map:
            snapshot.state.status = status_map[event_type]

        # Update snapshot with version incremented
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

    def _get_state_for_campaign_alias(self, campaign_id: uuid.UUID) -> CampaignState | None:
        with self._lock:
            # campaign_id = self._campaign_aliases.get(campaign_id_raw)
            # if campaign_id is None:
            # return None
            return self._campaigns.get(campaign_id)

    def _get_task_from_campaign(self, campaign: Campaign, step_id: CampaignStepId) -> Task | None:
        """Find a task in the campaign by its ID."""
        for task_group in campaign.task_groups:
            for task in task_group.tasks:
                try:
                    task_id = task.id
                    if task_id == step_id:
                        return task
                except (TypeError, ValueError):
                    if task.id == str(step_id):
                        return task
        return None

    def _build_task_group_executions(self, campaign: Campaign) -> list[TaskGroupExecution]:
        """Build task group execution descriptors from the campaign.

        Each task group's objectives list is converted into ObjectiveChecker
        instances.  The orchestrator does not need to know which specific
        objective types are present — it simply asks ``objectives_met()``
        after each iteration.
        """
        executions: list[TaskGroupExecution] = []
        for task_group in campaign.task_groups:
            task_ids = [task.id for task in task_group.tasks]
            checkers = _build_checkers(task_group.objectives)

            executions.append(
                TaskGroupExecution(
                    task_group_id=task_group.id,
                    task_ids=task_ids,
                    objective_checkers=checkers,
                )
            )
        return executions

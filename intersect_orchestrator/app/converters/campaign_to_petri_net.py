"""
Converter for mapping Campaign models to Petri Nets.

This module provides conversion from the Campaign data model to snakes.nets.PetriNet,
enabling state tracking and workflow management of campaigns using Petri Net theory.

Objectives are modeled as:
- Task-group level: MaxRuntime and ObjectiveIterate constraints tracked via metadata
- Campaign level: MaxRuntime and threshold constraints tracked via metadata
- Guards on transitions ensure constraints are respected during execution
"""

from typing import Any

from snakes import ConstraintError
from snakes.nets import PetriNet, Place, Transition, Value

from ..api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    MaxRuntime,
    ObjectiveAssert,
    ObjectiveIterate,
    TaskGroup,
    ThresholdRange,
    ThresholdUpperLimit,
)


class CampaignPetriNetConverter:
    """Converts Campaign models to Petri Nets for state management and execution tracking.

    The converter creates a Petri Net where:
    - Places represent states (Ready, TaskGroup execution states, Complete)
    - Transitions represent task groups and transitions between states
    - Tokens flow through the net based on task group execution and dependencies
    - Arc labels track which task groups must complete before others can start

    Example:
        >>> campaign = Campaign(...)
        >>> converter = CampaignPetriNetConverter()
        >>> petri_net = converter.convert(campaign)
    """

    def __init__(self) -> None:
        """Initialize the converter."""
        self.net: PetriNet | None = None
        self.campaign: Campaign | None = None
        self.task_group_map: dict[str, TaskGroup] = {}
        self.transition_map: dict[str, str] = {}  # Maps task group id to transition name
        self.places_created: set[str] = set()
        self.objectives_metadata: dict[str, dict[str, Any]] = {}  # Stores objective constraints
        self.campaign_objectives: dict[str, Any] = {}  # Campaign-level objectives

    def convert(self, campaign: Campaign) -> PetriNet:
        """Convert a Campaign to a Petri Net.

        Args:
            campaign: The Campaign model to convert

        Returns:
            PetriNet: A Petri Net representing the campaign's workflow

        Raises:
            ValueError: If the campaign has invalid structure or circular dependencies
        """
        self.campaign = campaign
        self.task_group_map = {tg.id: tg for tg in campaign.task_groups}

        # Create the net
        self.net = PetriNet(f'Campaign_{campaign.id}')

        # Validate and build the net
        self._validate_campaign()
        self._extract_objectives()
        self._create_places()
        self._create_transitions()
        self._create_arcs()
        self._initialize_tokens()

        return self.net

    def _validate_campaign(self) -> None:
        """Validate campaign structure for circular dependencies and consistency.

        Raises:
            ValueError: If circular dependencies are detected in task groups
        """
        # Check for circular dependencies in task groups
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def has_cycle(task_group_id: str) -> bool:
            visited.add(task_group_id)
            rec_stack.add(task_group_id)

            tg = self.task_group_map.get(task_group_id)
            if not tg:
                return False

            for dep_id in tg.group_dependencies:
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True

            rec_stack.remove(task_group_id)
            return False

        for tg_id in self.task_group_map:
            if tg_id not in visited and has_cycle(tg_id):
                msg = f'Circular dependency detected in task groups involving {tg_id}'
                raise ValueError(msg) from None

    def _extract_objectives(self) -> None:
        """Extract and store objectives from campaign and task groups.

        Organizes objectives for later use in guards and constraints:
        - Task-group objectives: MaxRuntime, ObjectiveIterate
        - Campaign objectives: MaxRuntime, threshold constraints
        """
        if not self.campaign:
            return

        # Extract task-group level objectives
        for tg in self.campaign.task_groups:
            tg_objectives = {
                'max_runtimes': [],
                'iterations': [],
                'thresholds': [],
            }

            if tg.objectives:
                for obj in tg.objectives:
                    if isinstance(obj, MaxRuntime):
                        tg_objectives['max_runtimes'].append(
                            {
                                'id': obj.id,
                                'max_time_seconds': obj.max_time.total_seconds(),
                                'max_time': obj.max_time,
                            }
                        )
                    elif isinstance(obj, ObjectiveIterate):
                        tg_objectives['iterations'].append(
                            {
                                'id': obj.id,
                                'iterations': obj.iterations,
                            }
                        )

            self.objectives_metadata[tg.id] = tg_objectives

        # Extract campaign-level objectives
        if self.campaign.objectives:
            campaign_obj = self.campaign.objectives

            # Campaign max runtime
            if campaign_obj.max_runtime:
                self.campaign_objectives['max_runtimes'] = [
                    {
                        'id': obj.id,
                        'max_time_seconds': obj.max_time.total_seconds(),
                        'max_time': obj.max_time,
                    }
                    for obj in campaign_obj.max_runtime
                ]
            else:
                self.campaign_objectives['max_runtimes'] = []

            # Campaign thresholds
            if campaign_obj.threshold:
                thresholds = []
                for thresh in campaign_obj.threshold:
                    thresh_info = {
                        'id': thresh.id,
                        'type': type(thresh).__name__,
                    }
                    if isinstance(thresh, ThresholdUpperLimit | ThresholdRange):
                        thresh_info.update(
                            {
                                'var': thresh.var,
                                'target': thresh.target,
                                'task_group': thresh.task_group,
                            }
                        )
                    elif isinstance(thresh, ObjectiveAssert):
                        thresh_info.update(
                            {
                                'var': thresh.var,
                                'target': thresh.target,
                                'type_enum': thresh.type,
                            }
                        )
                    thresholds.append(thresh_info)
                self.campaign_objectives['thresholds'] = thresholds
            else:
                self.campaign_objectives['thresholds'] = []

    def _create_places(self) -> None:
        """Create places in the Petri Net.

        Creates:
        - 'Ready': Initial state
        - Per task group states (e.g., 'tg_{id}_pending', 'tg_{id}_running', 'tg_{id}_complete')
        - 'Complete': Final state
        """
        # Initial place
        self.net.add_place(Place('Ready'))
        self.places_created.add('Ready')

        # Per-task-group places for tracking state
        for tg_id in self.task_group_map:
            # Pending: waiting for dependencies to complete
            pending_place = f'tg_{tg_id}_pending'
            self.net.add_place(Place(pending_place))
            self.places_created.add(pending_place)

            # Running: task group is executing
            running_place = f'tg_{tg_id}_running'
            self.net.add_place(Place(running_place))
            self.places_created.add(running_place)

            # Complete: task group finished
            complete_place = f'tg_{tg_id}_complete'
            self.net.add_place(Place(complete_place))
            self.places_created.add(complete_place)

            # Per-task completion places for tracking task dependencies
            for task in self.task_group_map[tg_id].tasks:
                task_complete_place = f'task_{tg_id}_{task.id}_complete'
                self.net.add_place(Place(task_complete_place))
                self.places_created.add(task_complete_place)

        # Final state
        self.net.add_place(Place('Complete'))
        self.places_created.add('Complete')

    def _create_transitions(self) -> None:
        """Create transitions in the Petri Net.

        Creates transitions for:
        - Activating task groups (when dependencies are met)
        - Completing task groups
        - Finalizing the campaign
        """
        # Transition to activate each task group
        for tg_id in self.task_group_map:
            activate_trans = f'activate_{tg_id}'
            self.net.add_transition(Transition(activate_trans))
            self.transition_map[f'{tg_id}_activate'] = activate_trans

            # Transition to mark task group as complete
            complete_trans = f'complete_{tg_id}'
            self.net.add_transition(Transition(complete_trans))
            self.transition_map[f'{tg_id}_complete'] = complete_trans

            # Task transitions inside the task group
            for task in self.task_group_map[tg_id].tasks:
                task_trans = f'task_{tg_id}_{task.id}'
                self.net.add_transition(Transition(task_trans))

        # Final transition to complete the campaign
        finalize_trans = 'finalize_campaign'
        self.net.add_transition(Transition(finalize_trans))
        self.transition_map['finalize'] = finalize_trans

    def _create_arcs(self) -> None:
        """Create arcs connecting places and transitions.

        Handles:
        - Initial activation of independent task groups from 'Ready'
        - Dependency-based activation (task groups wait for their dependencies)
        - Completion flow from task group complete to next task groups
        - Final completion arc to 'Complete' place
        """
        # Identify independent task groups (no dependencies)
        independent_tgs = {
            tg_id
            for tg_id in self.task_group_map
            if not self.task_group_map[tg_id].group_dependencies
        }

        # For each task group, create the activation and completion flow
        for tg_id, tg in self.task_group_map.items():
            activate_trans = self.transition_map[f'{tg_id}_activate']
            complete_trans = self.transition_map[f'{tg_id}_complete']

            pending_place = f'tg_{tg_id}_pending'
            complete_place = f'tg_{tg_id}_complete'

            # Handle activation inputs
            if tg_id in independent_tgs:
                # Independent task groups: activate from Ready
                self.net.add_input('Ready', activate_trans, Value(1))
            else:
                # Dependent task groups: wait for all dependencies to complete
                for dep_id in tg.group_dependencies:
                    dep_complete_place = f'tg_{dep_id}_complete'
                    self.net.add_input(dep_complete_place, activate_trans, Value(1))
                    self.net.add_output(dep_complete_place, activate_trans, Value(1))

            # Activation output: token to pending place
            self.net.add_output(pending_place, activate_trans, Value(1))

            # Completion inputs: from pending place
            self.net.add_input(pending_place, complete_trans, Value(1))

            # Task-level execution within the task group
            for task in tg.tasks:
                task_trans = f'task_{tg_id}_{task.id}'
                task_complete_place = f'task_{tg_id}_{task.id}_complete'

                # Task transitions require the task group to be active
                self.net.add_input(pending_place, task_trans, Value(1))
                self.net.add_output(pending_place, task_trans, Value(1))

                # Task dependencies within the task group
                for dep_task_id in task.task_dependencies:
                    dep_complete_place = f'task_{tg_id}_{dep_task_id}_complete'
                    self.net.add_input(dep_complete_place, task_trans, Value(1))
                    self.net.add_output(dep_complete_place, task_trans, Value(1))

                # Mark task as complete
                self.net.add_output(task_complete_place, task_trans, Value(1))

                # Task group completion requires all tasks to be complete
                self.net.add_input(task_complete_place, complete_trans, Value(1))

            # Completion output: token to complete place
            self.net.add_output(complete_place, complete_trans, Value(1))

        # Finalize transition: all task groups must be complete
        finalize_trans = self.transition_map['finalize']
        for tg_id in self.task_group_map:
            complete_place = f'tg_{tg_id}_complete'
            self.net.add_input(complete_place, finalize_trans, Value(1))

        # Finalize output: token to final Complete place
        self.net.add_output('Complete', finalize_trans, Value(1))

    def _initialize_tokens(self) -> None:
        """Initialize the Petri Net with tokens.

        Adds one token to the 'Ready' place to start the workflow.
        """
        ready_place = self.net.place('Ready')
        ready_place.add(1)

    def get_messaging_config(self) -> dict[str, dict[str, str]]:
        """Generate messaging configuration from the Petri Net.

        Creates a mapping of transitions to messaging topics for external communication.
        This allows the workflow engine to publish/subscribe to messages about
        task group state changes.

        Returns:
            Dict mapping transition names to messaging configuration with:
            - 'publish_topic': Topic for publishing task group start events
            - 'subscribe_topic': Topic for subscribing to task group completion events
        """
        if not self.campaign:
            msg = 'Campaign not set. Call convert() first.'
            raise ValueError(msg) from None

        messaging_config: dict[str, dict[str, str]] = {}

        for tg_id in self.task_group_map:
            campaign_id = self.campaign.id

            # Config for activation transition
            activate_trans = f'activate_{tg_id}'
            messaging_config[activate_trans] = {
                'publish_topic': f'campaign/{campaign_id}/task_group/{tg_id}/start',
                'subscribe_topic': f'campaign/{campaign_id}/task_group/{tg_id}/started',
            }

            # Config for completion transition
            complete_trans = f'complete_{tg_id}'
            messaging_config[complete_trans] = {
                'publish_topic': f'campaign/{campaign_id}/task_group/{tg_id}/complete',
                'subscribe_topic': f'campaign/{campaign_id}/task_group/{tg_id}/completed',
            }

        # Config for finalize transition
        finalize_trans = self.transition_map['finalize']
        messaging_config[finalize_trans] = {
            'publish_topic': f'campaign/{self.campaign.id}/finalize',
            'subscribe_topic': f'campaign/{self.campaign.id}/finalized',
        }

        return messaging_config

    def get_enabled_transitions(self) -> list[str]:
        """Get list of currently enabled transitions in the Petri Net.

        Returns:
            List of transition names that are currently enabled
        """
        if not self.net:
            msg = 'Petri Net not created. Call convert() first.'
            raise ValueError(msg) from None

        return [t.name for t in self.net.transition() if t.enabled(self.net)]

    def get_current_state(self) -> dict[str, int]:
        """Get the current state (marking) of the Petri Net.

        Returns:
            Dictionary mapping place names to number of tokens
        """
        if not self.net:
            msg = 'Petri Net not created. Call convert() first.'
            raise ValueError(msg) from None

        return {p.name: len(list(p.tokens)) for p in self.net.place()}

    def fire_transition(self, transition_name: str) -> None:
        """Fire a transition in the Petri Net, advancing the workflow.

        Args:
            transition_name: The name of the transition to fire

        Raises:
            ValueError: If transition is not enabled or doesn't exist
        """
        if not self.net:
            msg = 'Petri Net not created. Call convert() first.'
            raise ValueError(msg) from None

        try:
            trans = self.net.transition(transition_name)
            if not trans.enabled(self.net):
                msg = f"Transition '{transition_name}' is not enabled"
                raise ValueError(msg) from None
            trans.fire(self.net)
        except (KeyError, ConstraintError) as err:
            msg = f"Transition '{transition_name}' does not exist in the Petri Net"
            raise ValueError(msg) from err

    def get_taskgroup_objectives(self, task_group_id: str) -> dict[str, Any]:
        """Get objectives for a specific task group.

        Args:
            task_group_id: The ID of the task group

        Returns:
            Dictionary containing max_runtimes and iterations for the task group
        """
        if not self.objectives_metadata:
            msg = 'Objectives not extracted. Call convert() first.'
            raise ValueError(msg) from None

        return self.objectives_metadata.get(task_group_id, {})

    def get_campaign_objectives(self) -> dict[str, Any]:
        """Get campaign-level objectives.

        Returns:
            Dictionary containing max_runtimes and thresholds for the campaign
        """
        if not self.campaign_objectives:
            msg = 'Objectives not extracted. Call convert() first.'
            raise ValueError(msg) from None

        return self.campaign_objectives

    def get_all_objectives(self) -> dict[str, Any]:
        """Get all objectives from the campaign.

        Returns:
            Dictionary with taskgroup_objectives and campaign_objectives
        """
        if not self.objectives_metadata:
            msg = 'Objectives not extracted. Call convert() first.'
            raise ValueError(msg) from None

        return {
            'taskgroup_objectives': self.objectives_metadata,
            'campaign_objectives': self.campaign_objectives,
        }

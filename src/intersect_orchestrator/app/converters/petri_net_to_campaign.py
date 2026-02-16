"""
Reverse converter for mapping Petri Nets back to Campaign models.

This module provides conversion from snakes.nets.PetriNet back to Campaign data models,
enabling round-trip conversion and validation of campaign workflows.
"""

import datetime
from typing import Any

from snakes.nets import PetriNet

from ..api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    MaxRuntime,
    Objective,
    ObjectiveIterate,
    TaskGroup,
)


class PetriNetToCampaignConverter:
    """Converts Petri Nets back to Campaign models.

    This reverse converter reconstructs Campaign models from Petri Nets,
    preserving task group structure, dependencies, and objectives.

    Note: Since Petri Nets don't inherently store task details, this converter
    requires the original Campaign model to reconstruct full task information.
    It can, however, reconstruct the structure and dependencies from the net.

    Example:
        >>> petri_net = ... # Some PetriNet
        >>> original_campaign = ... # Original Campaign model
        >>> converter = PetriNetToCampaignConverter()
        >>> reconstructed_campaign = converter.convert(petri_net, original_campaign)
    """

    def __init__(self) -> None:
        """Initialize the converter."""
        self.net: PetriNet | None = None
        self.original_campaign: Campaign | None = None
        self.objectives_metadata: dict[str, Any] = {}

    def convert(
        self,
        petri_net: PetriNet,
        original_campaign: Campaign,
        objectives_metadata: dict[str, Any] | None = None,
    ) -> Campaign:
        """Convert a Petri Net back to a Campaign.

        Args:
            petri_net: The Petri Net to convert
            original_campaign: The original Campaign model (for task details)
            objectives_metadata: Optional objectives metadata from the forward converter

        Returns:
            Campaign: A reconstructed Campaign model

        Raises:
            ValueError: If the Petri Net structure is invalid or incompatible
        """
        self.net = petri_net
        self.original_campaign = original_campaign
        if objectives_metadata:
            self.objectives_metadata = objectives_metadata

        # Validate the net structure
        self._validate_petri_net_structure()

        # Extract task groups and dependencies from the net
        task_groups = self._extract_task_groups()

        # Reconstruct the campaign
        campaign = Campaign(
            id=original_campaign.id,
            name=original_campaign.name,
            user=original_campaign.user,
            description=original_campaign.description,
            task_groups=task_groups,
            objectives=original_campaign.objectives,
            inputs=original_campaign.inputs,
            outputs=original_campaign.outputs,
        )

        return campaign

    def _validate_petri_net_structure(self) -> None:
        """Validate that the Petri Net matches expected campaign structure.

        Raises:
            ValueError: If the net structure doesn't match expected pattern
        """
        if not self.net or not self.original_campaign:
            msg = 'Petri Net and original campaign must be set'
            raise ValueError(msg) from None

        # Check for required places
        place_names = {p.name for p in self.net.place()}
        required_places = {'Ready', 'Complete'}
        if not required_places.issubset(place_names):
            msg = f'Missing required places: {required_places - place_names}'
            raise ValueError(msg) from None

        # Check that we have expected task group places
        expected_tg_ids = {tg.id for tg in self.original_campaign.task_groups}
        for tg_id in expected_tg_ids:
            required_tg_places = {f'tg_{tg_id}_pending', f'tg_{tg_id}_complete'}
            if not required_tg_places.issubset(place_names):
                msg = (
                    f'Missing required places for task group {tg_id}: '
                    f'{required_tg_places - place_names}'
                )
                raise ValueError(msg) from None

    def _extract_task_groups(self) -> list[TaskGroup]:
        """Extract task groups from the Petri Net using original campaign data.

        Returns:
            List of reconstructed TaskGroup objects
        """
        task_groups = []

        for original_tg in self.original_campaign.task_groups:
            tg_id = original_tg.id

            # Reconstruct dependencies from the net
            dependencies = self._extract_dependencies_for_taskgroup(tg_id)

            # Get objectives from metadata if available
            objectives = []
            if tg_id in self.objectives_metadata.get('taskgroup_objectives', {}):
                objectives = self._reconstruct_objectives(
                    self.objectives_metadata['taskgroup_objectives'][tg_id]
                )

            # Create the task group with original task data but reconstructed dependencies
            task_group = TaskGroup(
                id=original_tg.id,
                group_dependencies=dependencies,
                tasks=original_tg.tasks,  # Use original task data
                objectives=objectives if objectives else original_tg.objectives,
            )

            task_groups.append(task_group)

        return task_groups

    def _extract_dependencies_for_taskgroup(self, task_group_id: str) -> list[str]:
        """Extract dependencies for a task group from the Petri Net.

        Args:
            task_group_id: The ID of the task group

        Returns:
            List of task group IDs that this one depends on
        """
        dependencies = []

        # Find the activate transition for this task group
        activate_trans_name = f'activate_{task_group_id}'
        try:
            activate_trans = self.net.transition(activate_trans_name)
        except KeyError:
            return dependencies

        # Get input places for this transition
        input_places = [p for p in self.net.place() if activate_trans in p.outgoing]

        # Parse place names to find dependencies
        for place in input_places:
            place_name = place.name

            # If it's a "complete" place, extract the task group ID
            if place_name.startswith('tg_') and place_name.endswith('_complete'):
                # Extract task group ID from place name
                dep_tg_id = place_name[3:-9]  # Remove "tg_" prefix and "_complete" suffix
                if dep_tg_id != 'campaign':  # Skip campaign-level complete place
                    dependencies.append(dep_tg_id)

        return dependencies

    def _reconstruct_objectives(self, objectives_info: dict[str, Any]) -> list[Objective]:
        """Reconstruct objectives from metadata.

        Args:
            objectives_info: Dictionary of objectives metadata

        Returns:
            List of reconstructed Objective objects
        """
        objectives = []

        # Reconstruct max runtime objectives
        for max_runtime_info in objectives_info.get('max_runtimes', []):
            max_time_seconds = max_runtime_info.get('max_time_seconds', 0)
            objectives.append(
                MaxRuntime(
                    id=max_runtime_info.get('id', ''),
                    max_time=datetime.timedelta(seconds=max_time_seconds),
                    task_group=max_runtime_info.get('task_group', ''),
                )
            )

        # Reconstruct iterate objectives
        for iterate_info in objectives_info.get('iterations', []):
            objectives.append(
                ObjectiveIterate(
                    id=iterate_info.get('id', ''),
                    type=iterate_info.get('type', 'iterate'),
                    iterations=iterate_info.get('iterations', 0),
                )
            )

        return objectives

    def get_extracted_dependencies(self) -> dict[str, list[str]]:
        """Get extracted dependencies for all task groups.

        Returns:
            Dictionary mapping task group IDs to lists of dependency IDs
        """
        if not self.original_campaign:
            msg = 'Original campaign not set. Call convert() first.'
            raise ValueError(msg) from None

        dependencies_map = {}
        for tg in self.original_campaign.task_groups:
            dependencies_map[tg.id] = self._extract_dependencies_for_taskgroup(tg.id)

        return dependencies_map

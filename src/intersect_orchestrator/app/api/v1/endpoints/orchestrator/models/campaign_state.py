"""Pydantic models representing runtime campaign state."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Self

from pydantic import Field

from .campaign import (
    Campaign,
    Objective,
    ObjectiveAssert,
    ObjectiveIterate,
    Task,
    TaskGroup,
)


class ExecutionStatus(str, Enum):
    """Execution status for campaign elements."""

    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETE = 'complete'
    ERROR = 'error'


class ObjectiveAssertState(ObjectiveAssert):
    """Objective assertion with state."""

    status: ExecutionStatus = ExecutionStatus.QUEUED


class ObjectiveIterateState(ObjectiveIterate):
    """Objective iterate with state."""

    status: ExecutionStatus = ExecutionStatus.QUEUED


class ObjectiveState(Objective):
    """Campaign-level objectives with state."""

    status: ExecutionStatus = ExecutionStatus.QUEUED


class TaskState(Task):
    """Task with execution state."""

    status: ExecutionStatus = ExecutionStatus.QUEUED


class TaskGroupState(TaskGroup):
    """Task group with execution state."""

    status: ExecutionStatus = ExecutionStatus.QUEUED
    tasks: Annotated[list[TaskState], Field(default_factory=list)]
    objectives: Annotated[
        list[ObjectiveAssertState | ObjectiveIterateState], Field(default_factory=list)
    ]


class CampaignState(Campaign):
    """Campaign with execution state."""

    status: ExecutionStatus = ExecutionStatus.QUEUED
    task_groups: Annotated[list[TaskGroupState], Field(default_factory=list)]
    objectives: ObjectiveState | None = None

    @classmethod
    def from_campaign(
        cls,
        campaign: Campaign,
        status: ExecutionStatus = ExecutionStatus.QUEUED,
    ) -> Self:
        """Create a campaign state snapshot from a Campaign model."""
        task_group_states: list[TaskGroupState] = []

        for task_group in campaign.task_groups:
            task_states = [
                TaskState(**task.model_dump(by_alias=True), status=status)
                for task in task_group.tasks
            ]
            objective_states = []
            for objective in task_group.objectives:
                if isinstance(objective, ObjectiveAssert):
                    objective_states.append(
                        ObjectiveAssertState(**objective.model_dump(), status=status)
                    )
                else:
                    objective_states.append(
                        ObjectiveIterateState(**objective.model_dump(), status=status)
                    )

            task_group_states.append(
                TaskGroupState(
                    **task_group.model_dump(exclude={'tasks', 'objectives'}),
                    tasks=task_states,
                    objectives=objective_states,
                    status=status,
                )
            )

        objective_state = None
        if campaign.objectives is not None:
            objective_state = ObjectiveState(
                **campaign.objectives.model_dump(),
                status=status,
            )

        return cls(
            **campaign.model_dump(exclude={'task_groups', 'objectives'}),
            task_groups=task_group_states,
            objectives=objective_state,
            status=status,
        )

"""
Pydantic models that represent a campaign.

These are from the most recent orchestrator iteration.
"""

import datetime
import uuid
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, Field, field_validator, model_validator

from .......utils.validation import validate_schema

IntersectCampaignId = uuid.UUID
CampaignStepId = uuid.UUID

# Campaign objective
# ----------------------------------------------------------------------------


class ThresholdUpperLimit(BaseModel):
    """Upper limit threshold for the objective.

    Attributes:
        id: The ID for the range.
        var: The variable associated with the objective.
        type: The type of threshold.
        target: The target value.
        task_group: The task group for this objective.
    """

    id: str
    var: str
    type: Literal['upper_limit']
    target: int = Field(gt=0, le=20)
    task_group: str


class ThresholdRange(BaseModel):
    """Range threshold for the objective.

    Attributes:
        id: The ID for the range.
        var: The variable associated with the objective.
        type: The type of threshold.
        target: The target value.
        task_group: The task group for this objective.
    """

    id: str
    var: str
    type: Literal['range']
    target: float = Field(gt=1.62, lt=3.14)
    task_group: str


class MaxRuntime(BaseModel):
    """Maximum runtime for an objective.

    Attributes:
        id: The ID for the max runtime.
        max_time: Time duration as ISO 8601 format.
        task_group: The task group for this objective.
    """

    id: str
    max_time: datetime.timedelta
    task_group: str


class Objective(BaseModel):
    """Objective for the campaign.

    Attributes:
        max_runtime: Max allowed runtime for a task group.
        threshold: Thresholds for a task group.
    """

    max_runtime: Annotated[list[MaxRuntime], Field(default_factory=list)]
    threshold: Annotated[list[ThresholdUpperLimit | ThresholdRange], Field(default_factory=list)]


# Task group and task
# ----------------------------------------------------------------------------


class Value(BaseModel):
    """Value for inputs and outputs.

    Attributes:
        id: The ID for the value.
        var: The associated variable.
    """

    id: str
    var: str


class Input(BaseModel):
    """Input for a task.

    Attributes:
        json_schema: The schema for the input.
        values: The values for the input.
    """

    json_schema: Annotated[
        dict[str, Any],
        Field(alias='schema'),
    ]  # using an alias because of a deprecated Pydantic namespace
    values: Annotated[list[Value], Field(min_length=1)]

    @field_validator('json_schema')
    @classmethod
    def _check_schema(cls, v: dict[str, Any]) -> dict[str, Any]:
        errors = validate_schema(v)
        if errors:
            raise ValueError(str(errors))
        return v


class Output(BaseModel):
    """Output for a task.

    Attributes:
        json_schema: The schema for the output.
        values: The values for the output.
    """

    json_schema: Annotated[
        dict[str, Any], Field(alias='schema')
    ]  # using an alias because of a deprecated Pydantic namespace
    values: Annotated[list[Value], Field(min_length=1)]

    @field_validator('json_schema')
    @classmethod
    def _check_schema(cls, v: dict[str, Any]) -> dict[str, Any]:
        errors = validate_schema(v)
        if errors:
            raise ValueError(str(errors))
        return v


class ObjectiveAssert(BaseModel):
    """Assertion for objectives.

    Attributes:
        id: The ID for the assertion objective.
        type: The type of assertion.
        var: The variable for the assertion.
        target: The target boolean.
    """

    id: str
    type: str
    """TODO make this an enum"""
    var: str
    target: bool


class ObjectiveIterate(BaseModel):
    """Iterations for an objective.

    Attributes:
        id: The ID for the iterations.
        type: The type of iteration.
        iterations: The number of iterations for the objective.
    """

    id: str
    type: str
    """TODO make this an enum"""
    iterations: int


class Task(BaseModel):
    """An individual task in a task group.

    Attributes:
        id: The ID for the task.
        hierarchy: The hierarchy of the task.
        capability: The Capability of the task.
        operation_id: Operation of the task. Mutually exclusive with event_name.
        event_name: Name of the event to listen to. Mutually exclusive with operation_id.
        output: The optional output value for the task. If not defined, assume no output beyond generic INTERSECT metadata.
        input: The optional input value for the task. If not defined, assume no input.
        task_dependencies: Dependencies on other tasks.
        task_objectives: Objectives for the task.
    """

    id: str
    hierarchy: str
    capability: str
    operation_id: str | None = None
    event_name: str | None = None
    output: Output | None = None
    input: Input | None = None
    task_dependencies: Annotated[list[str], Field(default_factory=list)]
    task_objectives: Annotated[Objective | None, Field(None)]

    @model_validator(mode='after')
    def _validate_task(self) -> Self:
        errors = []

        if bool(self.event_name) == bool(self.operation_id):
            errors.append(
                f'Task {self.id} needs to define exactly one of operation_id or event_name'
            )

        if errors:
            raise ValueError('\n'.join(errors))
        return self


class TaskGroup(BaseModel):
    """Task group.

    Attributes:
        id: The ID for the task group.
        group_dependencies: Dependencies on other task groups.
        task: The tasks that make up the task group.
        objectives: The objectives for the task group.
    """

    id: str
    group_dependencies: Annotated[list[str], Field(default_factory=list)]
    tasks: Annotated[list[Task], Field(default_factory=list)]
    objectives: Annotated[list[ObjectiveAssert | ObjectiveIterate], Field(default_factory=list)]


# Campaign
# ----------------------------------------------------------------------------


class Campaign(BaseModel):
    """Main campaign model.

    Attributes:
        id: The ID of the campaign.
        name: The name of the campaign.
        user: The user of the campaign.
        description: The description of the campaign.
        task_groups: A group of tasks.
        objectives: Objectives for the campaign.
        inputs: Inputs for the campaign.
        outputs: Outputs for the campaign.
    """

    id: str
    name: str
    user: str
    description: str
    task_groups: Annotated[list[TaskGroup], Field(default_factory=list)]
    objectives: Annotated[Objective | None, Field(None)]
    inputs: Annotated[list[Input], Field(default_factory=list)]
    outputs: Annotated[list[Output], Field(default_factory=list)]

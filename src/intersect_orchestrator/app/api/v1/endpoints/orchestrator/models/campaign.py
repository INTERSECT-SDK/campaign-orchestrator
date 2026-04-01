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

    id: uuid.UUID
    var: str
    type: Literal['upper_limit']
    target: int = Field(gt=0, le=20)
    task_group: uuid.UUID


class ThresholdRange(BaseModel):
    """Range threshold for the objective.

    Attributes:
        id: The ID for the range.
        var: The variable associated with the objective.
        type: The type of threshold.
        target: The target value.
        task_group: The task group for this objective.
    """

    id: uuid.UUID
    var: str
    type: Literal['range']
    target: float = Field(gt=1.62, lt=3.14)
    task_group: uuid.UUID


class MaxRuntime(BaseModel):
    """Maximum runtime for an objective.

    Attributes:
        id: The ID for the max runtime.
        max_time: Time duration as ISO 8601 format.
        task_group: The task group for this objective.
    """

    id: uuid.UUID
    max_time: datetime.timedelta
    task_group: uuid.UUID


class Objective(BaseModel):
    """Objective for the campaign.

    Attributes:
        max_runtime: Max allowed runtime for a task group.
        threshold: Thresholds for a task group.
    """

    max_runtime: Annotated[list[MaxRuntime], Field(default_factory=list)]
    threshold: Annotated[
        list[Annotated[ThresholdUpperLimit | ThresholdRange, Field(discriminator='type')]],
        Field(default_factory=list),
    ]


# Task group and task
# ----------------------------------------------------------------------------


class Value(BaseModel):
    """Value for inputs and outputs.

    Attributes:
        id: The ID for the value.
        var: The associated variable.
    """

    id: uuid.UUID
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

    id: uuid.UUID
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

    id: uuid.UUID
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

    id: uuid.UUID
    """ID for the task. If the task is a Request/Reply task, this ID should be unique between a Request/Reply 'pairs'."""
    hierarchy: str
    """URI PREFIX of the INTERSECT Service which should execute this task. Note that this is not a full URI; the full URI will generally end with 'request', 'response', or 'events', but is not captured in this field."""
    capability: str
    """Name of the INTERSECT Capability associated with this Task. Capabilities are used in both Request/Reply and Event messages. Capabilities are the top-level domain science objects."""
    operation_id: str | None = None
    """The operation ID is ONLY used for Request/Reply tasks, and for these tasks MUST be defined. This is the name of the function on the Service wrapped by '@intersect_message'.

    Each operation has an input schema and an output schema. Operations are namespaced to Capabilities."""
    event_name: str | None = None
    """The event name is ONLY used for Event tasks, and for these tasks MUST be defined. This is the name of the event type which gets emitted by the Service through 'intersect_sdk_emit_event'.

    Each event name has an output schema. Event names are namespaced to Capabilities.
    """
    output: Output | None = None
    input: Input | None = None
    task_dependencies: Annotated[list[uuid.UUID], Field(default_factory=list)]
    """List of other Task IDs WITHIN A TASK GROUP which must finish before this Task can start. Empty dependencies means this task can start immediately."""
    task_objectives: Annotated[Objective | None, Field(None)] = None
    """Sub-goals or limitations of a Task."""

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

    id: uuid.UUID
    """Unique ID for the task group. These are generally only used by the Orchestrator."""
    group_dependencies: Annotated[list[uuid.UUID], Field(default_factory=list)]
    """List of other Task Group IDs which must finish before this Task Group can start. Empty group dependencies means this task group can start immediately."""
    tasks: Annotated[list[Task], Field(min_length=1)]
    objectives: Annotated[list[ObjectiveAssert | ObjectiveIterate], Field(default_factory=list)]
    """Sub-goals or limitations of a Task Group."""


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

    id: uuid.UUID
    """The ID of the campaign. This is the primary identifier used to obtain both the campaign and any supplemental data.

    This ID should be sent across Request/Reply messages. Services should NEVER modify this ID.
    """
    name: str
    """TODO consider removing"""
    user: str
    """TODO consider removing"""
    description: str = ''
    """Human-readable description of the campaign."""
    task_groups: Annotated[list[TaskGroup], Field(min_length=1)]
    """Task groups are effectively 'mini-campaigns' within a campaign."""
    objectives: Annotated[Objective | None, Field(None)]
    """Objectives are sub-goals or limitations of a campaign."""
    inputs: Annotated[list[Input], Field(default_factory=list)]
    outputs: Annotated[list[Output], Field(default_factory=list)]

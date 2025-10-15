from typing import Literal

from pydantic import BaseModel, Field

from .icmp import CampaignStepId, IntersectCampaignId


class StepStartEvent(BaseModel):
    """This event is emitted when a campaign step has begun."""

    event_type: Literal['STEP_START'] = 'STEP_START'
    step_id: CampaignStepId


class StepCompleteEvent(BaseModel):
    """This event is emitted when a campaign step has completed."""

    event_type: Literal['STEP_COMPLETE'] = 'STEP_COMPLETE'
    step_id: CampaignStepId


class CampaignCompleteEvent(BaseModel):
    """This event is emitted when the entire campaign has been completed successfully. This event corresponds with the removal of the campaign."""

    event_type: Literal['CAMPAIGN_COMPLETE'] = 'CAMPAIGN_COMPLETE'


class ReadyForUserInputEvent(BaseModel):
    """This event is emitted when the user has received a suggested input, but would like to be 'in-the-loop' to manually verify this input, and potentially change it.

    TODO - we may not necessarily want this to stop the campaign, but it would make a lot of sense to do so.
    """

    event_type: Literal['READY_FOR_INPUT'] = 'READY_FOR_INPUT'
    fields_to_populate: list[str]
    """Paths to the fields we need user input from."""


class CampaignErrorFromServiceEvent(BaseModel):
    """This event emits if an Intersect Service sends back an error message. This will stop the campaign."""

    event_type: Literal['CAMPAIGN_ERROR_FROM_SERVICE'] = 'CAMPAIGN_ERROR_FROM_SERVICE'
    step_id: CampaignStepId
    service_hierarchy: str
    """URI of the Service which caused the error"""
    exception_message: str


class CampaignErrorSchemaIncompatibilityEvent(BaseModel):
    """This event emits in the event that a Service emits an output to be used as an input to another Service, but we are unable to convert the output to match the second Service's schema. This stops the campaign."""

    event_type: Literal['CAMPAIGN_ERROR_SCHEMA'] = 'CAMPAIGN_ERROR_SCHEMA'
    step_id: CampaignStepId
    exception_message: str


class UnknownErrorEvent(BaseModel):
    """This event emits if a campaign is unable to complete for whatever reason. This generally should be thought of as the fault of INTERSECT (i.e. the broker), and not the microservice or any user inputs."""

    event_type: Literal['UNKNOWN_ERROR'] = 'UNKNOWN_ERROR'
    exception_message: str


class OrchestratorEvent(BaseModel):
    """Campaigns periodically"""

    campaign_id: IntersectCampaignId
    """this is the ID of the campaign which emitted the event"""
    event: (
        StepStartEvent
        | StepCompleteEvent
        | CampaignCompleteEvent
        | CampaignErrorFromServiceEvent
        | CampaignErrorSchemaIncompatibilityEvent
        | ReadyForUserInputEvent
        | UnknownErrorEvent
    ) = Field(discriminator='event_type')
    """This is the event itself. All event types will have a field named 'event_type' which can be used for assistance in parsing this output."""

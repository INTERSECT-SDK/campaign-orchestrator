"""
Definitions of a campaign according to iHub
"""

import datetime
import uuid
from typing import Any, Literal

from pydantic import BaseModel

IntersectSchema = dict[str, Any]
IntersectCampaignId = uuid.UUID
CampaignStepId = uuid.UUID


class Capability(BaseModel):
    name: str
    created_at: datetime.datetime
    last_lifecycle_message: datetime.datetime | None = None
    service_id: int
    endpoints_schema: IntersectSchema


class CapabilityData(BaseModel):
    capability: Capability
    endpoint: str
    endpoint_channel: IntersectSchema
    # make sure to ignore "position", "measured", "selected", "dragging"; these are UI traits


class Node(BaseModel):
    id: IntersectCampaignId
    type: Literal['capability']  # does this ever vary from "capability"?
    data: CapabilityData


class Edge(BaseModel):
    pass


class Icmp(BaseModel):
    """This is the core data type stored by iHub"""

    nodes: list[Node]
    edges: list[Edge]
    metadata: dict[str, Any]

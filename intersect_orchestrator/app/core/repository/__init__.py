"""Campaign repository package exports."""

from . import factory
from .base import (
    CampaignEvent,
    CampaignRepository,
    CampaignSnapshot,
    _require_psycopg,
    _require_pymongo,
)
from .in_memory import InMemoryCampaignRepository
from .mongo import MongoCampaignRepository
from .postgres import PostgresCampaignRepository

__all__ = [
    'CampaignEvent',
    'CampaignRepository',
    'CampaignSnapshot',
    'factory',
    'InMemoryCampaignRepository',
    'MongoCampaignRepository',
    'PostgresCampaignRepository',
    '_require_pymongo',
    '_require_psycopg',
]

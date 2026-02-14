"""Campaign repository package exports."""

from . import factory
from .base import (
    CampaignEvent,
    CampaignRepository,
    CampaignSnapshot,
    require_psycopg,
    require_pymongo,
)
from .in_memory import InMemoryCampaignRepository
from .mongo import MongoCampaignRepository
from .postgres import PostgresCampaignRepository

__all__ = [
    'CampaignEvent',
    'CampaignRepository',
    'CampaignSnapshot',
    'InMemoryCampaignRepository',
    'MongoCampaignRepository',
    'PostgresCampaignRepository',
    'factory',
    'require_psycopg',
    'require_pymongo',
]

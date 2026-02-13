"""Repository factory for backend selection."""

from __future__ import annotations

import sys
from typing import Any

from .base import _require_psycopg, _require_pymongo
from .in_memory import InMemoryCampaignRepository
from .mongo import MongoCampaignRepository
from .postgres import PostgresCampaignRepository


def create_campaign_repository(settings: Any):
    """Create campaign repository from settings.

    Expected settings fields:
    - CAMPAIGN_REPOSITORY_BACKEND: memory|mongo|postgres
    - CAMPAIGN_REPOSITORY_MONGO_URI
    - CAMPAIGN_REPOSITORY_MONGO_DB
    - CAMPAIGN_REPOSITORY_POSTGRES_DSN
    """
    backend = str(getattr(settings, 'CAMPAIGN_REPOSITORY_BACKEND', 'memory')).lower()

    if backend == 'memory':
        return InMemoryCampaignRepository()

    if backend == 'mongo':
        mongo_client, _ = _require_pymongo()
        uri = getattr(settings, 'CAMPAIGN_REPOSITORY_MONGO_URI', 'mongodb://localhost:27017')
        db_name = getattr(settings, 'CAMPAIGN_REPOSITORY_MONGO_DB', 'intersect_orchestrator')
        client = mongo_client(uri)
        return MongoCampaignRepository(client, db_name=db_name)

    if backend == 'postgres':
        _require_psycopg()
        dsn = getattr(
            settings,
            'CAMPAIGN_REPOSITORY_POSTGRES_DSN',
            'postgresql://user:pass@localhost:5432/intersect_orchestrator',
        )
        psycopg = sys.modules.get('psycopg')
        if psycopg is None:
            import psycopg as psycopg  # type: ignore[redefined-outer-name]
        connection = psycopg.connect(dsn)
        return PostgresCampaignRepository(connection)

    msg = f'Unsupported campaign repository backend: {backend}'
    raise ValueError(msg)

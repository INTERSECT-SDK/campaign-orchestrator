from __future__ import annotations

import types
from types import SimpleNamespace

import pytest

from intersect_orchestrator.app.core.repository import (
    InMemoryCampaignRepository,
    MongoCampaignRepository,
)
from intersect_orchestrator.app.core.repository import factory as repository_factory


def _settings(**overrides):
    defaults = {
        'CAMPAIGN_REPOSITORY_BACKEND': 'memory',
        'CAMPAIGN_REPOSITORY_MONGO_URI': 'mongodb://localhost:27017',
        'CAMPAIGN_REPOSITORY_MONGO_DB': 'intersect_orchestrator',
        'CAMPAIGN_REPOSITORY_POSTGRES_DSN': 'postgresql://user:pass@localhost:5432/db',
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_create_repository_defaults_to_memory() -> None:
    repo = repository_factory.create_campaign_repository(_settings())
    assert isinstance(repo, InMemoryCampaignRepository)


def test_create_repository_mongo(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCollection:
        def create_index(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def __getitem__(self, _name):
            return FakeCollection()

    class FakeMongoClient:
        def __init__(self, uri: str):
            self.uri = uri

        def __getitem__(self, _name):
            return FakeDB()

    monkeypatch.setattr(repository_factory, 'require_pymongo', lambda: (FakeMongoClient, 1))

    settings = _settings(
        CAMPAIGN_REPOSITORY_BACKEND='mongo',
        CAMPAIGN_REPOSITORY_MONGO_URI='mongodb://fake',
        CAMPAIGN_REPOSITORY_MONGO_DB='test_db',
    )
    repo = repository_factory.create_campaign_repository(settings)

    assert isinstance(repo, MongoCampaignRepository)
    assert repo._client.uri == 'mongodb://fake'


def test_create_repository_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_psycopg = types.SimpleNamespace()

    def fake_connect(dsn: str):
        return {'dsn': dsn}

    fake_psycopg.connect = fake_connect
    monkeypatch.setattr(repository_factory, 'require_psycopg', lambda: object())
    monkeypatch.setitem(repository_factory.sys.modules, 'psycopg', fake_psycopg)

    class FakePostgresRepository:
        def __init__(self, connection):
            self.connection = connection

    monkeypatch.setattr(repository_factory, 'PostgresCampaignRepository', FakePostgresRepository)

    settings = _settings(
        CAMPAIGN_REPOSITORY_BACKEND='postgres',
        CAMPAIGN_REPOSITORY_POSTGRES_DSN='postgresql://fake',
    )
    repo = repository_factory.create_campaign_repository(settings)

    assert isinstance(repo, FakePostgresRepository)
    assert repo.connection['dsn'] == 'postgresql://fake'


def test_create_repository_unknown_backend() -> None:
    settings = _settings(CAMPAIGN_REPOSITORY_BACKEND='unknown')
    with pytest.raises(ValueError, match='Unsupported campaign repository backend'):
        repository_factory.create_campaign_repository(settings)

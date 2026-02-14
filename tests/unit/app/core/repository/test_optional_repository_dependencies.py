from __future__ import annotations

import pytest

from intersect_orchestrator.app.core import repository as repo
from intersect_orchestrator.app.core.repository import mongo as repo_mongo
from intersect_orchestrator.app.core.repository import postgres as repo_postgres


def test_mongo_repository_requires_pymongo(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise():
        msg = 'pymongo is required'
        raise ImportError(msg)

    monkeypatch.setattr(repo_mongo, 'require_pymongo', _raise)

    with pytest.raises(ImportError, match='pymongo is required'):
        repo.MongoCampaignRepository(client=object(), db_name='test')


def test_postgres_repository_requires_psycopg(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise():
        msg = 'psycopg is required'
        raise ImportError(msg)

    monkeypatch.setattr(repo_postgres, 'require_psycopg', _raise)

    with pytest.raises(ImportError, match='psycopg is required'):
        repo.PostgresCampaignRepository(connection=object())

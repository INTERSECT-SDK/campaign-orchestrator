from __future__ import annotations

import pytest

from intersect_orchestrator.app.core import repository as repo
from intersect_orchestrator.app.core.repository import base as repo_base


def test_mongo_repository_requires_pymongo(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise():
        raise ImportError('pymongo is required')

    monkeypatch.setattr(repo_base, '_require_pymongo', _raise)

    with pytest.raises(ImportError, match='pymongo is required'):
        repo.MongoCampaignRepository(client=object(), db_name='test')


def test_postgres_repository_requires_psycopg(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise():
        raise ImportError('psycopg is required')

    monkeypatch.setattr(repo_base, '_require_psycopg', _raise)

    with pytest.raises(ImportError, match='psycopg is required'):
        repo.PostgresCampaignRepository(connection=object())

from __future__ import annotations

import uuid

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    ObjectiveIterate,
)
from intersect_orchestrator.app.core.objective_checkers import IterateChecker


class TestIterateChecker:
    def test_not_met_initially(self) -> None:
        obj = ObjectiveIterate(id=uuid.uuid4(), type='iterate', iterations=3)
        checker = IterateChecker(obj)
        assert not checker.is_met()

    def test_met_after_exact_iterations(self) -> None:
        obj = ObjectiveIterate(id=uuid.uuid4(), type='iterate', iterations=3)
        checker = IterateChecker(obj)

        checker.record_iteration({})
        checker.record_iteration({})
        assert not checker.is_met()

        checker.record_iteration({})
        assert checker.is_met()

    def test_still_met_after_extra_iterations(self) -> None:
        obj = ObjectiveIterate(id=uuid.uuid4(), type='iterate', iterations=1)
        checker = IterateChecker(obj)

        checker.record_iteration({})
        assert checker.is_met()

        checker.record_iteration({})
        assert checker.is_met()

    def test_objective_id_matches(self) -> None:
        oid = uuid.uuid4()
        obj = ObjectiveIterate(id=oid, type='iterate', iterations=5)
        checker = IterateChecker(obj)
        assert checker.objective_id == oid

    def test_single_iteration(self) -> None:
        obj = ObjectiveIterate(id=uuid.uuid4(), type='iterate', iterations=1)
        checker = IterateChecker(obj)
        assert not checker.is_met()
        checker.record_iteration({})
        assert checker.is_met()

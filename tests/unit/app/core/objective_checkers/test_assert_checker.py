from __future__ import annotations

import uuid

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    ObjectiveAssert,
)
from intersect_orchestrator.app.core.objective_checkers import AssertChecker


class TestAssertChecker:
    def test_not_met_initially(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)
        assert not checker.is_met()

    def test_met_when_var_matches_target_true(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)

        checker.record_iteration({uuid.uuid4(): b'{"success": true}'})
        assert checker.is_met()

    def test_not_met_when_var_is_false(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)

        checker.record_iteration({uuid.uuid4(): b'{"success": false}'})
        assert not checker.is_met()

    def test_met_when_target_is_false_and_var_is_false(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='done', target=False)
        checker = AssertChecker(obj)

        checker.record_iteration({uuid.uuid4(): b'{"done": false}'})
        assert checker.is_met()

    def test_stays_met_once_satisfied(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)

        checker.record_iteration({uuid.uuid4(): b'{"success": true}'})
        assert checker.is_met()

        # Even a subsequent non-matching payload doesn't un-meet it
        checker.record_iteration({uuid.uuid4(): b'{"success": false}'})
        assert checker.is_met()

    def test_ignores_non_json_payloads(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)

        checker.record_iteration({uuid.uuid4(): b'not json'})
        assert not checker.is_met()

    def test_ignores_missing_var(self) -> None:
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)

        checker.record_iteration({uuid.uuid4(): b'{"other_key": true}'})
        assert not checker.is_met()

    def test_checks_all_task_payloads(self) -> None:
        """If any task's payload satisfies the assertion, the checker is met."""
        obj = ObjectiveAssert(id=uuid.uuid4(), type='assert', var='success', target=True)
        checker = AssertChecker(obj)

        checker.record_iteration(
            {
                uuid.uuid4(): b'{"success": false}',
                uuid.uuid4(): b'{"success": true}',
            }
        )
        assert checker.is_met()

    def test_objective_id_matches(self) -> None:
        oid = uuid.uuid4()
        obj = ObjectiveAssert(id=oid, type='assert', var='x', target=True)
        checker = AssertChecker(obj)
        assert checker.objective_id == oid

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from .base import ObjectiveChecker

if TYPE_CHECKING:
    import uuid

    from ...api.v1.endpoints.orchestrator.models.campaign import ObjectiveAssert


class AssertChecker(ObjectiveChecker):
    """Satisfied when the watched variable in the payload equals the target boolean.

    The checker looks for the variable named ``var`` in every task's JSON
    response each iteration and evaluates ``bool(value) == target``.
    It is met as soon as the assertion holds after any completed iteration.
    """

    def __init__(self, objective: ObjectiveAssert) -> None:
        self._objective = objective
        self._met = False

    @property
    def objective_id(self) -> uuid.UUID:
        return self._objective.id

    def record_iteration(self, iteration_results: dict[uuid.UUID, bytes]) -> None:
        if self._met:
            return
        for payload_bytes in iteration_results.values():
            try:
                data = json.loads(payload_bytes)
            except (json.JSONDecodeError, TypeError):
                continue
            if (
                isinstance(data, dict)
                and self._objective.var in data
                and bool(data[self._objective.var]) == self._objective.target
            ):
                self._met = True
                return

    def is_met(self) -> bool:
        return self._met

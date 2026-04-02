from __future__ import annotations

from typing import TYPE_CHECKING

from .base import ObjectiveChecker

if TYPE_CHECKING:
    import uuid

    from ...api.v1.endpoints.orchestrator.models.campaign import ObjectiveIterate


class IterateChecker(ObjectiveChecker):
    """Satisfied after a fixed number of iterations."""

    def __init__(self, objective: ObjectiveIterate) -> None:
        self._objective = objective
        self._iterations_completed: int = 0

    @property
    def objective_id(self) -> uuid.UUID:
        return self._objective.id

    def record_iteration(self, iteration_results: dict[uuid.UUID, bytes]) -> None:
        del iteration_results
        self._iterations_completed += 1

    def is_met(self) -> bool:
        return self._iterations_completed >= self._objective.iterations

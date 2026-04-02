from __future__ import annotations

import abc
import uuid


class ObjectiveChecker(abc.ABC):
    """Base class for task-group objective checkers."""

    @property
    @abc.abstractmethod
    def objective_id(self) -> uuid.UUID:
        """Return the campaign-model ID of the objective being tracked."""

    @abc.abstractmethod
    def record_iteration(self, iteration_results: dict[uuid.UUID, bytes]) -> None:
        """Called once per completed iteration with a mapping of
        task_id -> raw response payload for every task in the group."""

    @abc.abstractmethod
    def is_met(self) -> bool:
        """Return *True* when the objective's completion criteria are satisfied."""

"""Objective checkers for task-group completion criteria.

The orchestrator calls ``is_met()`` on each checker after every completed
iteration; when *all* checkers report ``True`` the task group is done.
"""

from .assert_checker import AssertChecker
from .base import ObjectiveChecker
from .iterate_checker import IterateChecker

__all__ = [
    "AssertChecker",
    "IterateChecker",
    "ObjectiveChecker",
]

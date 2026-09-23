"""INTERSECT capability for running mock long-running tasks.

Copied from the iHub mock-capabilities repository
(https://code.ornl.gov/intersect/ihub/mock-capabilities, commit 725d9d3,
src/mock_long_running_tasks/long_running_tasks.py).
"""

import datetime
import logging
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, ClassVar, Literal

from intersect_sdk import (
    HierarchyConfig,
    IntersectBaseCapabilityImplementation,
    IntersectCapabilityError,
    IntersectEventDefinition,
    IntersectService,
    IntersectServiceConfig,
    default_intersect_lifecycle_loop,
    intersect_message,
    intersect_status,
)
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

LOCAL_TASK_INTERVAL_SECONDS = 5
LOCAL_TASK_BUSY_MESSAGE = "Local long-running task is already running"


def long_running_task(
    total_seconds: int,
    progress_callback: Callable[[int], None] | None = None,
) -> None:
    """Run a mock long-running task and log progress at regular intervals."""
    elapsed_seconds = 0

    # Log the current time and remaining seconds until the task duration is complete.
    while elapsed_seconds < total_seconds:
        time.sleep(LOCAL_TASK_INTERVAL_SECONDS)
        elapsed_seconds += LOCAL_TASK_INTERVAL_SECONDS
        remaining_seconds = max(total_seconds - elapsed_seconds, 0)
        current_time = (
            datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        )
        logger.info(
            "Local task: current time %s, %s seconds remaining",
            current_time,
            remaining_seconds,
        )
        progress = elapsed_seconds * 100 // total_seconds
        if progress_callback is not None and progress < 100:
            progress_callback(progress)


class LongRunningTasksCapabilityState(BaseModel):
    """Current state of the long-running-task capability."""

    task_state: Annotated[
        Literal["idle", "running", "completed", "failed"],
        Field(description="Current lifecycle state of the most recent task."),
    ] = "idle"


@dataclass
class RunTaskLocalRequest:
    """Input payload for running a mock long-running task locally."""

    minutes: Annotated[
        int,
        Field(
            title="minutes",
            description="Positive local task duration in minutes.",
            gt=0,
        ),
    ]


@dataclass
class RunTaskResponse:
    """Response from the long-running-task capability."""

    state: LongRunningTasksCapabilityState
    status: int | None


@dataclass
class TaskProgressEvent:
    """Progress updates for the local long-running task."""

    operation: Literal["run_task_local"]
    status: Literal["running", "completed"]
    progress: Annotated[
        int,
        Field(
            description="Whole-number completion percentage.",
            ge=0,
            le=100,
        ),
    ]


@dataclass
class TaskCompletedEvent:
    """One-shot signal that a local long-running task completed successfully."""

    operation: Literal["run_task_local"]
    status: Literal["completed"]


class LongRunningTasksCapabilityImpl(IntersectBaseCapabilityImplementation):
    """Run mock long-running tasks through INTERSECT."""

    intersect_sdk_capability_name = "Long_Running_Tasks"

    # Define the events that this capability can emit to the INTERSECT broker.
    intersect_sdk_events: ClassVar[dict[str, IntersectEventDefinition]] = {
        "task_progress": IntersectEventDefinition(
            event_type=TaskProgressEvent,
            event_documentation=(
                "Reports percentage progress while a local long-running task is active."
            ),
        ),
        "task_completed": IntersectEventDefinition(
            event_type=TaskCompletedEvent,
            event_documentation=(
                "One-shot signal emitted after a local long-running task completes "
                "successfully."
            ),
        ),
    }

    def __init__(self) -> None:
        """Initialize the capability state and process-local task lock."""
        super().__init__()
        self.state = LongRunningTasksCapabilityState()
        self._local_task_lock = threading.Lock()

    @intersect_status()
    def status(self) -> LongRunningTasksCapabilityState:
        """Return the capability's current task state."""
        logger.debug("Status requested, current state: %s", self.state)
        return self.state

    def _acquire_local_task_lock(
        self,
        operation: Literal["run_task_local"],
    ) -> None:
        """Acquire the process-local task lock or reject the invocation."""
        if self._local_task_lock.acquire(blocking=False):
            return

        logger.error(
            "Local long-running task is already running, rejecting %s",
            operation,
        )
        raise IntersectCapabilityError(LOCAL_TASK_BUSY_MESSAGE)

    @intersect_message()
    def run_task_local(
        self,
        request: RunTaskLocalRequest,
    ) -> RunTaskResponse:
        """Run a synchronous long-running task within the capability process."""
        self._acquire_local_task_lock("run_task_local")

        try:
            total_seconds = request.minutes * 60
            self.state.task_state = "running"
            self.intersect_sdk_emit_event(
                "task_progress",
                TaskProgressEvent(
                    operation="run_task_local",
                    status="running",
                    progress=0,
                ),
            )
            long_running_task(
                total_seconds,
                progress_callback=lambda progress: self.intersect_sdk_emit_event(
                    "task_progress",
                    TaskProgressEvent(
                        operation="run_task_local",
                        status="running",
                        progress=progress,
                    ),
                ),
            )
        except Exception:
            self.state.task_state = "failed"
            logger.exception("Local long-running task failed")
            raise
        else:
            self.state.task_state = "completed"
            self.intersect_sdk_emit_event(
                "task_progress",
                TaskProgressEvent(
                    operation="run_task_local",
                    status="completed",
                    progress=100,
                ),
            )
            self.intersect_sdk_emit_event(
                "task_completed",
                TaskCompletedEvent(
                    operation="run_task_local",
                    status="completed",
                ),
            )
            return RunTaskResponse(state=self.state, status=None)
        finally:
            self._local_task_lock.release()

    @staticmethod
    def run() -> None:
        """Start the broker-connected INTERSECT service."""
        broker_config = {
            "brokers": [
                {
                    "username": os.getenv("BROKER_USERNAME"),
                    "password": os.getenv("BROKER_PASSWORD"),
                    "host": os.getenv("BROKER_HOST"),
                    "port": os.getenv("BROKER_PORT"),
                    "protocol": os.getenv("BROKER_PROTOCOL"),
                },
            ],
        }
        config = IntersectServiceConfig(
            hierarchy=HierarchyConfig(
                organization="long-running-tasks-organization",
                facility="long-running-tasks-facility",
                system="long-running-tasks-system",
                subsystem="long-running-tasks-subsystem",
                service="long-running-tasks-service",
            ),
            status_interval=30.0,
            **broker_config,
        )
        capability = LongRunningTasksCapabilityImpl()
        service = IntersectService([capability], config)
        logger.info(
            "Starting %s, use Ctrl+C to exit.",
            LongRunningTasksCapabilityImpl.intersect_sdk_capability_name,
        )
        default_intersect_lifecycle_loop(service)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    LongRunningTasksCapabilityImpl.run()

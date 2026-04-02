"""End-to-end test for iterative campaign execution with the random number service.

These tests require:
  - A running RabbitMQ broker
  - The random-number-service connected to the same broker

Start both with:
    docker-compose up -d broker random-number-service

If the broker is not available, tests will be skipped gracefully.
"""

from __future__ import annotations

import json
import pathlib
import time
import uuid

import pytest

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign_state import (
    ExecutionStatus,
)
from intersect_orchestrator.app.core.campaign_orchestrator import CampaignOrchestrator
from intersect_orchestrator.app.core.repository import InMemoryCampaignRepository

TEST_DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'
ITERATIVE_CAMPAIGN_FILE = (
    TEST_DATA_DIR / 'campaign' / 'random-number-campaign-iterative.campaign.json'
)
EXPECTED_EVENTS_FILE = (
    TEST_DATA_DIR / 'target' / 'random-number-campaign-iterative.expected-events.json'
)


def _load_iterative_campaign_json() -> dict:
    with ITERATIVE_CAMPAIGN_FILE.open() as f:
        return json.load(f)


def _campaign_with_fresh_ids(campaign_data: dict) -> dict:
    """Return a deep-copied campaign payload with fresh IDs.

    This prevents stale broker replies from prior tests (with old request/campaign IDs)
    from colliding with the current test run when using the shared fixed queue.
    """
    data = json.loads(json.dumps(campaign_data))
    data['id'] = str(uuid.uuid4())

    for task_group in data.get('task_groups', []):
        task_group['id'] = str(uuid.uuid4())
        for task in task_group.get('tasks', []):
            task['id'] = str(uuid.uuid4())
        for objective in task_group.get('objectives') or []:
            objective['id'] = str(uuid.uuid4())

    for objective in data.get('objectives') or []:
        objective['id'] = str(uuid.uuid4())

    return data


@pytest.mark.integration
class TestIterativeCampaignE2E:
    """End-to-end tests for iterative (looping) campaign execution.

    Note: Tests in this class run sequentially to avoid RabbitMQ queue contention.
    The shared broker queue is drained between tests via the intersect_client_with_cleanup fixture.
    """

    def test_submit_iterative_campaign_creates_state(
        self, check_broker_available: None, intersect_client_with_cleanup
    ) -> None:
        """Submitting the iterative campaign should create state, Petri net, and initial events."""
        campaign_data = _campaign_with_fresh_ids(_load_iterative_campaign_json())
        campaign = Campaign(**campaign_data)

        repository = InMemoryCampaignRepository()
        orchestrator = CampaignOrchestrator(
            intersect_client=intersect_client_with_cleanup, repository=repository
        )

        campaign_id = orchestrator.submit_campaign(campaign)

        assert campaign_id is not None
        assert isinstance(campaign_id, uuid.UUID)

        stored = repository.load_snapshot(campaign_id)
        assert stored is not None
        assert stored.campaign_id == campaign_id

        events = list(repository.load_events(campaign_id))
        assert len(events) > 0

        # Should have CAMPAIGN_STARTED and TASK_GROUP_STARTED at minimum
        event_types = [e.event_type for e in events]
        assert 'CAMPAIGN_STARTED' in event_types
        assert 'TASK_GROUP_STARTED' in event_types

    def test_iterative_campaign_completes_all_iterations(
        self, check_broker_available: None, intersect_client_with_cleanup
    ) -> None:
        """Full loop: submit iterative campaign, random-number-service replies,
        orchestrator iterates 10 times over 2 parallel tasks, campaign completes.

        Requires the random-number-service to be running and connected to the
        same broker (``docker-compose up -d broker random-number-service``).
        """
        campaign_data = _campaign_with_fresh_ids(_load_iterative_campaign_json())
        campaign = Campaign(**campaign_data)

        repository = InMemoryCampaignRepository()
        orchestrator = CampaignOrchestrator(
            intersect_client=intersect_client_with_cleanup, repository=repository
        )
        intersect_client_with_cleanup.set_campaign_orchestrator(orchestrator)

        campaign_id = orchestrator.submit_campaign(campaign)

        # Wait for campaign to complete (the random-number-service must reply).
        # 2 tasks x 10 iterations = 20 round-trips; generous timeout.
        timeout_seconds = 120
        poll_interval = 0.5
        elapsed = 0.0

        while elapsed < timeout_seconds:
            snapshot = repository.load_snapshot(campaign_id)
            if snapshot is None:
                # campaign was removed — means _finish_campaign or error ran
                break
            if snapshot.state.status in (ExecutionStatus.COMPLETE, ExecutionStatus.ERROR):
                break
            time.sleep(poll_interval)
            elapsed += poll_interval

        # ---- assertions ----

        # Collect all events recorded for this campaign
        events = list(repository.load_events(campaign_id))
        event_types = [e.event_type for e in events]

        # Campaign should have completed (not errored)
        assert 'CAMPAIGN_COMPLETED' in event_types, (
            f'Campaign did not complete. Events: {event_types}'
        )
        assert 'CAMPAIGN_ERROR' not in event_types

        # Load the expected event sequence from the target file.
        # This file is the single source of truth for the frontend event contract.
        with EXPECTED_EVENTS_FILE.open() as f:
            expected_event_types = json.load(f)['event_types']

        assert event_types == expected_event_types, (
            f'Event sequence mismatch.\n'
            f'  Expected ({len(expected_event_types)} events): {expected_event_types}\n'
            f'  Actual   ({len(event_types)} events): {event_types}'
        )

        # No duplicate seq numbers (event-sourcing invariant)
        seqs = [e.seq for e in events]
        assert len(seqs) == len(set(seqs)), 'Events must have unique seq numbers'

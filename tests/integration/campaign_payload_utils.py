"""Helpers for building unique campaign payloads in integration tests."""

from __future__ import annotations

import json
import uuid
from typing import Any


def campaign_with_fresh_ids(campaign_data: dict[str, Any]) -> dict[str, Any]:
    """Return a deep-copied campaign payload with fresh IDs.

    The function rewrites campaign, task-group, task, dependency, objective,
    and input/output value IDs so repeated test runs don't collide with prior
    orchestrator state or stale broker messages.
    """
    data = json.loads(json.dumps(campaign_data))
    data['id'] = str(uuid.uuid4())

    task_group_id_map: dict[str, str] = {}
    task_id_map: dict[str, str] = {}

    for task_group in data.get('task_groups', []):
        old_group_id = task_group['id']
        task_group_id_map[old_group_id] = str(uuid.uuid4())

    for task_group in data.get('task_groups', []):
        for task in task_group.get('tasks', []):
            old_task_id = task['id']
            task_id_map[old_task_id] = str(uuid.uuid4())

    for task_group in data.get('task_groups', []):
        task_group['id'] = task_group_id_map[task_group['id']]
        task_group['group_dependencies'] = [
            task_group_id_map.get(dep_id, dep_id)
            for dep_id in task_group.get('group_dependencies', [])
        ]

        for task in task_group.get('tasks', []):
            task['id'] = task_id_map[task['id']]
            task['task_dependencies'] = [
                task_id_map.get(dep_id, dep_id) for dep_id in task.get('task_dependencies', [])
            ]
            for value_set in ('input', 'output'):
                value_obj = task.get(value_set)
                if value_obj and value_obj.get('values'):
                    for value in value_obj['values']:
                        value['id'] = str(uuid.uuid4())

        for objective in task_group.get('objectives') or []:
            objective['id'] = str(uuid.uuid4())

    for objective in data.get('objectives') or []:
        objective['id'] = str(uuid.uuid4())

    return data

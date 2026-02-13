"""
Tests for loading and validating campaign JSON files with objectives.
"""

import json
from pathlib import Path

import pytest

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    ObjectiveIterate,
)
from tests import TEST_DATA_DIR


@pytest.fixture
def test_data_dir():
    """Get the path to test data directory."""
    return Path(TEST_DATA_DIR, 'campaign')


class TestCampaignDataFiles:
    """Test suite for validating campaign JSON data files."""

    def test_load_taskgroup_objectives_campaign(self, test_data_dir):
        """Test loading campaign with task-group level objectives."""
        file_path = test_data_dir / 'campaign-with-taskgroup-objectives.campaign.json'
        assert file_path.exists(), f'Test data file not found: {file_path}'

        with file_path.open() as f:
            data = json.load(f)

        # Validate structure
        assert data['id'] == 'campaign-with-taskgroup-objectives'
        assert len(data['task_groups']) == 3
        assert data['task_groups'][0]['id'] == 'measurement_setup'
        assert data['task_groups'][1]['id'] == 'data_collection'
        assert data['task_groups'][2]['id'] == 'data_analysis'

        # Validate dependencies
        assert data['task_groups'][0]['group_dependencies'] == []
        assert data['task_groups'][1]['group_dependencies'] == ['measurement_setup']
        assert data['task_groups'][2]['group_dependencies'] == ['data_collection']

        # Validate task-group level objectives
        setup_objectives = data['task_groups'][0]['objectives']
        assert len(setup_objectives) == 1
        assert setup_objectives[0]['type'] == 'iterate'
        assert setup_objectives[0]['iterations'] == 3

        collect_objectives = data['task_groups'][1]['objectives']
        assert len(collect_objectives) == 1
        assert collect_objectives[0]['type'] == 'iterate'
        assert collect_objectives[0]['iterations'] == 10

        analysis_objectives = data['task_groups'][2]['objectives']
        assert len(analysis_objectives) == 1
        assert analysis_objectives[0]['type'] == 'iterate'
        assert analysis_objectives[0]['iterations'] == 5

    def test_load_campaign_level_objectives_campaign(self, test_data_dir):
        """Test loading campaign with campaign-level objectives."""
        file_path = test_data_dir / 'campaign-with-campaign-objectives.campaign.json'
        assert file_path.exists(), f'Test data file not found: {file_path}'

        with file_path.open() as f:
            data = json.load(f)

        # Validate structure
        assert data['id'] == 'campaign-with-campaign-objectives'
        assert len(data['task_groups']) == 3
        assert data['task_groups'][0]['id'] == 'phase_one'
        assert data['task_groups'][1]['id'] == 'phase_two'
        assert data['task_groups'][2]['id'] == 'phase_three'

        # Validate campaign-level objectives
        objectives = data['objectives']
        assert objectives is not None
        assert 'max_runtime' in objectives
        assert len(objectives['max_runtime']) == 1
        assert objectives['max_runtime'][0]['max_time'] == 'PT1H'
        assert objectives['max_runtime'][0]['task_group'] == 'campaign'

    def test_load_threshold_objectives_campaign(self, test_data_dir):
        """Test loading campaign with threshold-based objectives."""
        file_path = test_data_dir / 'campaign-with-threshold-objectives.campaign.json'
        assert file_path.exists(), f'Test data file not found: {file_path}'

        with file_path.open() as f:
            data = json.load(f)

        # Validate structure
        assert data['id'] == 'campaign-with-threshold-objectives'
        assert len(data['task_groups']) == 2

        # Validate task-group level objectives
        collection_objectives = data['task_groups'][0]['objectives']
        assert len(collection_objectives) == 1
        assert collection_objectives[0]['type'] == 'iterate'
        assert collection_objectives[0]['iterations'] == 20

        # Validate campaign-level threshold objectives
        objectives = data['objectives']
        assert objectives is not None
        assert 'threshold' in objectives
        assert len(objectives['threshold']) == 2

        # Validate threshold types
        threshold_types = {t['type'] for t in objectives['threshold']}
        assert 'upper_limit' in threshold_types
        assert 'range' in threshold_types

        # Validate upper limit threshold
        upper_limit = next(t for t in objectives['threshold'] if t['type'] == 'upper_limit')
        assert upper_limit['var'] == 'temperature'
        assert upper_limit['target'] == 20

    def test_load_complex_campaign(self, test_data_dir):
        """Test loading complex campaign with mixed objectives."""
        file_path = test_data_dir / 'complex-campaign-all-objectives.campaign.json'
        assert file_path.exists(), f'Test data file not found: {file_path}'

        with file_path.open() as f:
            data = json.load(f)

        # Validate structure
        assert data['id'] == 'complex-campaign-all-objectives'
        assert len(data['task_groups']) == 4

        # Validate task group chain dependencies
        expected_deps = [
            [],
            ['sample_preparation'],
            ['scattering_measurement'],
            ['data_quality_check'],
        ]
        for tg, expected_dep in zip(data['task_groups'], expected_deps, strict=False):
            assert tg['group_dependencies'] == expected_dep

        # Validate mixed objectives across task groups
        assert len(data['task_groups'][0]['objectives']) == 1  # prep_iterate
        assert len(data['task_groups'][1]['objectives']) == 1  # scattering_iterate
        assert len(data['task_groups'][2]['objectives']) == 1  # quality_iterate
        assert len(data['task_groups'][3]['objectives']) == 0  # no objectives

        # Validate campaign-level objectives
        objectives = data['objectives']
        assert objectives is not None
        assert len(objectives['max_runtime']) == 4
        assert len(objectives['threshold']) == 2

    def test_parse_campaign_with_pydantic(self, test_data_dir):
        """Test that JSON can be parsed into Campaign Pydantic model."""
        file_path = test_data_dir / 'campaign-with-taskgroup-objectives.campaign.json'

        with file_path.open() as f:
            data = json.load(f)

        # Should not raise validation error
        campaign = Campaign(**data)

        assert campaign.id == 'campaign-with-taskgroup-objectives'
        assert len(campaign.task_groups) == 3

        # Verify task group objectives are loaded (should be ObjectiveIterate)
        assert len(campaign.task_groups[0].objectives) == 1
        assert isinstance(campaign.task_groups[0].objectives[0], ObjectiveIterate)
        assert campaign.task_groups[0].objectives[0].iterations == 3

        assert len(campaign.task_groups[1].objectives) == 1
        assert isinstance(campaign.task_groups[1].objectives[0], ObjectiveIterate)
        assert campaign.task_groups[1].objectives[0].iterations == 10

        # Verify campaign-level objectives
        assert campaign.objectives is not None
        assert len(campaign.objectives.max_runtime) == 2
        assert campaign.objectives.max_runtime[0].max_time.total_seconds() == 600  # 10 minutes
        assert campaign.objectives.max_runtime[1].max_time.total_seconds() == 300  # 5 minutes

    def test_parse_complex_campaign_with_pydantic(self, test_data_dir):
        """Test parsing complex campaign with all objective types."""
        file_path = test_data_dir / 'complex-campaign-all-objectives.campaign.json'

        with file_path.open() as f:
            data = json.load(f)

        campaign = Campaign(**data)

        assert campaign.id == 'complex-campaign-all-objectives'
        assert len(campaign.task_groups) == 4

        # Verify task group objectives
        assert len(campaign.task_groups[0].objectives) == 1
        assert isinstance(campaign.task_groups[0].objectives[0], ObjectiveIterate)

        assert len(campaign.task_groups[1].objectives) == 1
        assert isinstance(campaign.task_groups[1].objectives[0], ObjectiveIterate)

        # Verify campaign-level objectives contain both max_runtime and thresholds
        assert campaign.objectives is not None
        assert len(campaign.objectives.max_runtime) == 4
        assert len(campaign.objectives.threshold) == 2

        # Validate threshold types
        threshold_types = {type(t).__name__ for t in campaign.objectives.threshold}
        assert 'ThresholdRange' in threshold_types
        assert 'ThresholdUpperLimit' in threshold_types

    def test_taskgroup_dependencies_form_dag(self, test_data_dir):
        """Test that task group dependencies form a valid DAG (no cycles)."""
        file_path = test_data_dir / 'complex-campaign-all-objectives.campaign.json'

        with file_path.open() as f:
            data = json.load(f)

        campaign = Campaign(**data)

        # Create adjacency list
        tg_map = {tg.id: tg for tg in campaign.task_groups}

        # DFS to detect cycles
        def has_cycle(node_id, visited, rec_stack):
            visited.add(node_id)
            rec_stack.add(node_id)

            tg = tg_map.get(node_id)
            if tg:
                for dep_id in tg.group_dependencies:
                    if dep_id not in visited:
                        if has_cycle(dep_id, visited, rec_stack):
                            return True
                    elif dep_id in rec_stack:
                        return True

            rec_stack.remove(node_id)
            return False

        visited: set = set()
        for tg_id in tg_map:
            if tg_id not in visited:
                assert not has_cycle(tg_id, visited, set()), f'Cycle detected involving {tg_id}'

    def test_objectives_variables_referenced(self, test_data_dir):
        """Test that objective variables are defined in task outputs."""
        file_path = test_data_dir / 'complex-campaign-all-objectives.campaign.json'

        with file_path.open() as f:
            data = json.load(f)

        campaign = Campaign(**data)

        # Collect all variables referenced in campaign-level objectives
        referenced_vars = set()
        if campaign.objectives:
            for threshold in campaign.objectives.threshold:
                if hasattr(threshold, 'var'):
                    referenced_vars.add(threshold.var)

        # Collect all output variables
        output_vars = set()
        for tg in campaign.task_groups:
            for task in tg.tasks:
                if task.output:
                    for value in task.output.values:
                        output_vars.add(value.var)

        # All referenced variables should be defined in outputs
        assert referenced_vars.issubset(output_vars), (
            f'Variables referenced in objectives but not defined in outputs: '
            f'{referenced_vars - output_vars}'
        )

    def test_campaign_objectives_all_files(self, test_data_dir):
        """Test that all test campaign files can be loaded without errors."""
        campaign_files = [
            'campaign-with-taskgroup-objectives.campaign.json',
            'campaign-with-campaign-objectives.campaign.json',
            'campaign-with-threshold-objectives.campaign.json',
            'complex-campaign-all-objectives.campaign.json',
        ]

        for filename in campaign_files:
            file_path = test_data_dir / filename
            assert file_path.exists(), f'Test data file not found: {file_path}'

            with file_path.open() as f:
                data = json.load(f)

            # Should parse without errors
            campaign = Campaign(**data)
            assert campaign.id is not None
            assert len(campaign.task_groups) > 0

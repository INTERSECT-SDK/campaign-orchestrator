"""
Tests for the campaign to Petri Net converter.
"""

import datetime
from uuid import UUID

import pytest

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    MaxRuntime,
    Objective,
    ObjectiveIterate,
    Task,
    TaskGroup,
)
from intersect_orchestrator.app.converters.campaign_to_petri_net import (
    CampaignPetriNetConverter,
)


@pytest.fixture
def simple_campaign() -> Campaign:
    """Create a simple campaign with two independent task groups."""
    return Campaign(
        id=UUID('bd3d15a8-e57d-4e5a-9253-252cec3719df'),
        name='Simple Campaign',
        user='test_user',
        description='A simple test campaign',
        task_groups=[
            TaskGroup(
                id=UUID('46dd56c9-e96b-4841-986a-b37ec9d0fbd5'),
                group_dependencies=[],
                tasks=[
                    Task(
                        id=UUID('d87b1c5e-3c45-4349-a20c-f48e5096e5c5'),
                        hierarchy='1',
                        capability='capability_1',
                        operation_id='op_1',
                        event_name=None,
                    ),
                    Task(
                        id=UUID('2dfc53be-d599-4f19-b599-dacb62d03ba1'),
                        hierarchy='1',
                        capability='capability_1_followup',
                        operation_id='op_1_followup',
                        event_name=None,
                        task_dependencies=[UUID('d87b1c5e-3c45-4349-a20c-f48e5096e5c5')],
                    ),
                ],
                objectives=[
                    ObjectiveIterate(
                        id=UUID('8fa346b9-18cd-4003-8505-1cb49720a0d0'),
                        type='iterate',
                        iterations=1,
                    )
                ],
            ),
            TaskGroup(
                id=UUID('1317b8c3-ff66-4286-8b8d-ac9c57754afa'),
                group_dependencies=[],
                tasks=[
                    Task(
                        id=UUID('c27f9c48-ea25-4635-8eba-172270241db9'),
                        hierarchy='1',
                        capability='capability_2',
                        operation_id='op_2',
                        event_name=None,
                    ),
                    Task(
                        id=UUID('e4cb2098-d97b-4185-b81d-8dba17bc1ec8'),
                        hierarchy='1',
                        capability='capability_2_followup',
                        operation_id='op_2_followup',
                        event_name=None,
                        task_dependencies=[UUID('c27f9c48-ea25-4635-8eba-172270241db9')],
                    ),
                ],
                objectives=[
                    ObjectiveIterate(
                        id=UUID('9b015979-ca7b-4363-98df-0aa119117d21'),
                        type='iterate',
                        iterations=1,
                    )
                ],
            ),
        ],
    )


@pytest.fixture
def dependent_campaign() -> Campaign:
    """Create a campaign with dependent task groups."""
    return Campaign(
        id=UUID('c19c9e1d-533b-45fb-9485-7cff577cea57'),
        name='Dependent Campaign',
        user='test_user',
        description='A campaign with task group dependencies',
        task_groups=[
            TaskGroup(
                id=UUID('2e192e4c-fee6-4a98-bf75-26b36f59f483'),
                group_dependencies=[],
                tasks=[
                    Task(
                        id=UUID('d1c255cb-1106-4540-833d-fac45b4961fb'),
                        hierarchy='1',
                        capability='setup',
                        operation_id='op_setup',
                        event_name=None,
                    ),
                    Task(
                        id=UUID('90cc4d05-0aea-4704-b7b3-a1436c93e8e3'),
                        hierarchy='1',
                        capability='setup_validate',
                        operation_id='op_setup_validate',
                        event_name=None,
                        task_dependencies=[UUID('d1c255cb-1106-4540-833d-fac45b4961fb')],
                    ),
                ],
                objectives=[
                    ObjectiveIterate(
                        id=UUID('2a07acfc-76eb-45df-b97d-1b5004909dda'),
                        type='iterate',
                        iterations=1,
                    )
                ],
            ),
            TaskGroup(
                id=UUID('659ecf81-1f8c-445a-a57b-84f402180873'),
                group_dependencies=[UUID('2e192e4c-fee6-4a98-bf75-26b36f59f483')],
                tasks=[
                    Task(
                        id=UUID('54aa19ea-03f3-4792-8011-3e4c338fc5c6'),
                        hierarchy='1',
                        capability='process',
                        operation_id='op_process',
                        event_name=None,
                    ),
                    Task(
                        id=UUID('94e5be33-cf48-4557-a143-391ba844f896'),
                        hierarchy='1',
                        capability='process_validate',
                        operation_id='op_process_validate',
                        event_name=None,
                        task_dependencies=[UUID('54aa19ea-03f3-4792-8011-3e4c338fc5c6')],
                    ),
                ],
                objectives=[
                    ObjectiveIterate(
                        id=UUID('c9e10ae5-2020-464f-b8e2-ad44a47595a2'),
                        type='iterate',
                        iterations=1,
                    )
                ],
            ),
            TaskGroup(
                id=UUID('287ed9bf-51d9-4197-940c-99b8036ed5d3'),
                group_dependencies=[UUID('659ecf81-1f8c-445a-a57b-84f402180873')],
                tasks=[
                    Task(
                        id=UUID('9059baf8-9fe7-4098-a57e-1ad9a2ebdede'),
                        hierarchy='1',
                        capability='finalize',
                        operation_id='op_finalize',
                        event_name=None,
                    ),
                    Task(
                        id=UUID('b4bf7a97-94c7-4f2d-b8a7-fc734bd1c68f'),
                        hierarchy='1',
                        capability='finalize_validate',
                        operation_id='op_finalize_validate',
                        event_name=None,
                        task_dependencies=[UUID('9059baf8-9fe7-4098-a57e-1ad9a2ebdede')],
                    ),
                ],
                objectives=[
                    ObjectiveIterate(
                        id=UUID('c5d57e16-f1d1-46b3-8bb4-28c4b950a7c7'),
                        type='iterate',
                        iterations=1,
                    )
                ],
            ),
        ],
    )


@pytest.fixture
def circular_dependency_campaign() -> Campaign:
    """Create a campaign with circular dependencies (invalid)."""
    return Campaign(
        id=UUID('f1d76ff5-045c-4c0f-9a51-81b88acd510f'),
        name='Circular Campaign',
        user='test_user',
        description='A campaign with circular dependencies',
        task_groups=[
            TaskGroup(
                id=UUID('3dab387e-6ca5-430b-8f59-d6607b70fbcf'),
                group_dependencies=[UUID('34a47124-6b1c-46ff-8850-aaaea9e4aea7')],
                tasks=[
                    Task(
                        id=UUID('9059baf8-9fe7-4098-a57e-1ad9a2ebdede'),
                        hierarchy='1',
                        capability='finalize',
                        operation_id='op_finalize',
                    )
                ],
            ),
            TaskGroup(
                id=UUID('34a47124-6b1c-46ff-8850-aaaea9e4aea7'),
                group_dependencies=[UUID('955e781e-c3a3-4505-8f1c-300c3be52476')],
                tasks=[
                    Task(
                        id=UUID('b4bf7a97-94c7-4f2d-b8a7-fc734bd1c68f'),
                        hierarchy='1',
                        capability='finalize_validate',
                        operation_id='op_finalize_validate',
                    )
                ],
            ),
            TaskGroup(
                id=UUID('955e781e-c3a3-4505-8f1c-300c3be52476'),
                group_dependencies=[UUID('3dab387e-6ca5-430b-8f59-d6607b70fbcf')],
                tasks=[
                    Task(
                        id=UUID('c5d57e16-f1d1-46b3-8bb4-28c4b950a7c7'),
                        hierarchy='1',
                        capability='finalize_validate',
                        operation_id='op_finalize_validate',
                    )
                ],
            ),
        ],
    )


class TestCampaignPetriNetConverter:
    """Test suite for CampaignPetriNetConverter."""

    def test_converter_initialization(self):
        """Test that converter initializes correctly."""
        converter = CampaignPetriNetConverter()
        assert converter.net is None
        assert converter.campaign is None
        assert converter.task_group_map == {}
        assert converter.transition_map == {}
        assert converter.places_created == set()

    def test_simple_campaign_conversion(self, simple_campaign: Campaign):
        """Test conversion of a simple campaign."""
        converter = CampaignPetriNetConverter()
        net = converter.convert(simple_campaign)

        campaign_str = str(simple_campaign.id)
        tg_1 = str(simple_campaign.task_groups[0].id)
        tg_2 = str(simple_campaign.task_groups[1].id)
        task_1 = str(simple_campaign.task_groups[0].tasks[0].id)
        task_1_followup = str(simple_campaign.task_groups[0].tasks[1].id)
        task_2 = str(simple_campaign.task_groups[1].tasks[0].id)
        task_2_followup = str(simple_campaign.task_groups[1].tasks[1].id)

        # Verify net was created
        assert net is not None
        assert net.name == f'Campaign_{campaign_str}'

        # Verify places were created
        expected_places = {
            'Ready',
            f'tg_{tg_1}_pending',
            f'tg_{tg_1}_running',
            f'tg_{tg_1}_complete',
            f'task_{tg_1}_{task_1}_complete',
            f'task_{tg_1}_{task_1_followup}_complete',
            f'tg_{tg_2}_pending',
            f'tg_{tg_2}_running',
            f'tg_{tg_2}_complete',
            f'task_{tg_2}_{task_2}_complete',
            f'task_{tg_2}_{task_2_followup}_complete',
            'Complete',
        }
        actual_places = {p.name for p in net.place()}
        assert actual_places == expected_places

        # Verify transitions were created
        expected_transitions = {
            f'activate_{tg_1}',
            f'complete_{tg_1}',
            f'task_{tg_1}_{task_1}',
            f'task_{tg_1}_{task_1_followup}',
            f'activate_{tg_2}',
            f'complete_{tg_2}',
            f'task_{tg_2}_{task_2}',
            f'task_{tg_2}_{task_2_followup}',
            'finalize_campaign',
        }
        actual_transitions = {t.name for t in net.transition()}
        assert actual_transitions == expected_transitions

        # Verify initial state
        state = converter.get_current_state()
        assert state['Ready'] == 1

    def test_dependent_campaign_conversion(self, dependent_campaign: Campaign):
        """Test conversion of a campaign with dependencies."""
        tg_setup = str(dependent_campaign.task_groups[0].id)
        task_setup = str(dependent_campaign.task_groups[0].tasks[0].id)
        tg_process = str(dependent_campaign.task_groups[1].id)
        task_process = str(dependent_campaign.task_groups[1].tasks[0].id)
        tg_finalize = str(dependent_campaign.task_groups[2].id)
        task_finalize = str(dependent_campaign.task_groups[2].tasks[0].id)

        converter = CampaignPetriNetConverter()
        net = converter.convert(dependent_campaign)

        assert net is not None

        # Verify places and transitions exist
        places = {p.name for p in net.place()}
        assert 'Ready' in places
        assert f'tg_{tg_setup}_complete' in places
        assert f'tg_{tg_process}_pending' in places
        assert f'tg_{tg_finalize}_pending' in places
        assert f'task_{tg_setup}_{task_setup}_complete' in places
        assert f'task_{tg_process}_{task_process}_complete' in places
        assert f'task_{tg_finalize}_{task_finalize}_complete' in places
        assert 'Complete' in places

    def test_circular_dependency_detection(self, circular_dependency_campaign: Campaign):
        """Test that circular dependencies are detected and raise ValueError."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match='Circular dependency detected'):
            converter.convert(circular_dependency_campaign)

    def test_get_enabled_transitions_initial(self, simple_campaign: Campaign):
        """Test getting enabled transitions in initial state."""
        tg_1 = str(simple_campaign.task_groups[0].id)
        tg_2 = str(simple_campaign.task_groups[1].id)

        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        enabled = converter.get_enabled_transitions()

        # Both independent task groups should be activatable
        assert f'activate_{tg_1}' in enabled
        assert f'activate_{tg_2}' in enabled

    def test_get_current_state(self, simple_campaign: Campaign):
        """Test getting the current state of the Petri Net."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        state = converter.get_current_state()

        # Ready place should have 1 token
        assert state['Ready'] == 1

        # All other places should have 0 tokens
        for place_name in state:
            if place_name != 'Ready':
                assert state[place_name] == 0

    def test_fire_transition(self, simple_campaign: Campaign):
        """Test firing a transition in the Petri Net."""
        tg_1 = str(simple_campaign.task_groups[0].id)

        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        initial_state = converter.get_current_state()
        assert initial_state['Ready'] == 1
        assert initial_state[f'tg_{tg_1}_pending'] == 0

        # Fire activation transition for tg_1
        converter.fire_transition(f'activate_{tg_1}')

        state_after = converter.get_current_state()
        assert state_after['Ready'] == 0
        assert state_after[f'tg_{tg_1}_pending'] == 1

    def test_fire_disabled_transition_raises_error(self, simple_campaign: Campaign):
        """Test that firing a disabled transition raises ValueError."""
        tg_1 = str(simple_campaign.task_groups[0].id)

        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        # Try to fire a completion transition that's not enabled
        with pytest.raises(ValueError, match='is not enabled'):
            converter.fire_transition(f'complete_{tg_1}')

    def test_fire_nonexistent_transition_raises_error(self, simple_campaign: Campaign):
        """Test that firing a nonexistent transition raises ValueError."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        with pytest.raises(ValueError, match='does not exist'):
            converter.fire_transition('nonexistent_transition')

    def test_messaging_config_generation(self, simple_campaign: Campaign):
        """Test generating messaging configuration from the converter."""
        campaign_id = str(simple_campaign.id)
        tg_1 = str(simple_campaign.task_groups[0].id)
        tg_2 = str(simple_campaign.task_groups[1].id)

        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        config = converter.get_messaging_config()

        # Should have config for both task groups (activate + complete) + finalize
        assert f'activate_{tg_1}' in config
        assert f'complete_{tg_1}' in config
        assert f'activate_{tg_2}' in config
        assert f'complete_{tg_2}' in config
        assert 'finalize_campaign' in config

        # Check topic naming
        assert (
            config[f'activate_{tg_1}']['publish_topic']
            == f'campaign/{campaign_id}/task_group/{tg_1}/start'
        )
        assert (
            config[f'complete_{tg_1}']['subscribe_topic']
            == f'campaign/{campaign_id}/task_group/{tg_1}/completed'
        )

    def test_workflow_execution_simple(self, simple_campaign: Campaign):
        """Test a simple workflow execution through the Petri Net."""
        # Modify campaign to make tg_2 depend on tg_1
        simple_campaign.task_groups[1].group_dependencies = [
            UUID('46dd56c9-e96b-4841-986a-b37ec9d0fbd5')
        ]

        tg_1 = str(simple_campaign.task_groups[0].id)
        task_1 = str(simple_campaign.task_groups[0].tasks[0].id)
        task_1_followup = str(simple_campaign.task_groups[0].tasks[1].id)

        tg_2 = str(simple_campaign.task_groups[1].id)
        task_2 = str(simple_campaign.task_groups[1].tasks[0].id)
        task_2_followup = str(simple_campaign.task_groups[1].tasks[1].id)

        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        # Initial state: Ready has 1 token
        assert converter.get_current_state()['Ready'] == 1

        # Activate first task group
        converter.fire_transition(f'activate_{tg_1}')
        assert converter.get_current_state()[f'tg_{tg_1}_pending'] == 1

        # Execute tasks within first task group
        converter.fire_transition(f'task_{tg_1}_{task_1}')
        assert converter.get_current_state()[f'task_{tg_1}_{task_1}_complete'] == 1

        converter.fire_transition(f'task_{tg_1}_{task_1_followup}')
        assert converter.get_current_state()[f'task_{tg_1}_{task_1_followup}_complete'] == 1

        # Complete first task group
        converter.fire_transition(f'complete_{tg_1}')
        assert converter.get_current_state()[f'tg_{tg_1}_complete'] == 1

        # Now activate second task group (can only happen after tg_1 completes)
        converter.fire_transition(f'activate_{tg_2}')
        assert converter.get_current_state()[f'tg_{tg_2}_pending'] == 1

        # Execute tasks within second task group
        converter.fire_transition(f'task_{tg_2}_{task_2}')
        assert converter.get_current_state()[f'task_{tg_2}_{task_2}_complete'] == 1

        converter.fire_transition(f'task_{tg_2}_{task_2_followup}')
        assert converter.get_current_state()[f'task_{tg_2}_{task_2_followup}_complete'] == 1

        # Complete second task group
        converter.fire_transition(f'complete_{tg_2}')
        assert converter.get_current_state()[f'tg_{tg_2}_complete'] == 1

        # Finalize campaign
        converter.fire_transition('finalize_campaign')
        assert converter.get_current_state()['Complete'] == 1

    def test_workflow_execution_with_dependencies(self, dependent_campaign: Campaign):
        """Test workflow execution with task group dependencies."""
        tg_setup = str(dependent_campaign.task_groups[0].id)
        task_setup = str(dependent_campaign.task_groups[0].tasks[0].id)
        task_setup_validate = str(dependent_campaign.task_groups[0].tasks[1].id)

        tg_process = str(dependent_campaign.task_groups[1].id)
        task_process = str(dependent_campaign.task_groups[1].tasks[0].id)
        task_process_validate = str(dependent_campaign.task_groups[1].tasks[1].id)

        tg_finalize = str(dependent_campaign.task_groups[2].id)
        task_finalize = str(dependent_campaign.task_groups[2].tasks[0].id)
        task_finalize_validate = str(dependent_campaign.task_groups[2].tasks[1].id)

        converter = CampaignPetriNetConverter()
        converter.convert(dependent_campaign)

        # Only setup should be initially enabled
        enabled = converter.get_enabled_transitions()
        assert f'activate_{tg_setup}' in enabled
        assert f'activate_{tg_process}' not in enabled

        # Activate and complete setup
        converter.fire_transition(f'activate_{tg_setup}')
        converter.fire_transition(f'task_{tg_setup}_{task_setup}')
        converter.fire_transition(f'task_{tg_setup}_{task_setup_validate}')
        converter.fire_transition(f'complete_{tg_setup}')

        # Now process should be enabled
        enabled = converter.get_enabled_transitions()
        assert f'activate_{tg_process}' in enabled

        # Activate and complete process
        converter.fire_transition(f'activate_{tg_process}')
        converter.fire_transition(f'task_{tg_process}_{task_process}')
        converter.fire_transition(f'task_{tg_process}_{task_process_validate}')
        converter.fire_transition(f'complete_{tg_process}')

        # Now finalize should be enabled
        enabled = converter.get_enabled_transitions()
        assert f'activate_{tg_finalize}' in enabled

        # Activate and complete finalize
        converter.fire_transition(f'activate_{tg_finalize}')
        converter.fire_transition(f'task_{tg_finalize}_{task_finalize}')
        converter.fire_transition(f'task_{tg_finalize}_{task_finalize_validate}')
        converter.fire_transition(f'complete_{tg_finalize}')

        # Now finalize_campaign should be enabled
        enabled = converter.get_enabled_transitions()
        assert 'finalize_campaign' in enabled

        converter.fire_transition('finalize_campaign')
        assert converter.get_current_state()['Complete'] == 1

    def test_error_on_get_state_before_conversion(self):
        """Test that getting state before conversion raises error."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match='Petri Net not created'):
            converter.get_current_state()

    def test_error_on_fire_transition_before_conversion(self):
        """Test that firing transition before conversion raises error."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match='Petri Net not created'):
            converter.fire_transition('any_transition')

    def test_error_on_messaging_config_before_conversion(self):
        """Test that getting messaging config before conversion raises error."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match='Campaign not set'):
            converter.get_messaging_config()

    def test_objectives_extraction_simple(self, simple_campaign: Campaign):
        """Test that objectives are extracted from campaign."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        all_objectives = converter.get_all_objectives()
        assert 'taskgroup_objectives' in all_objectives
        assert 'campaign_objectives' in all_objectives

        tg_objectives = converter.get_taskgroup_objectives(
            UUID('46dd56c9-e96b-4841-986a-b37ec9d0fbd5')
        )
        assert len(tg_objectives['iterations']) == 1

    def test_objectives_with_max_runtime(self):
        """Test extraction of max runtime objectives at campaign level."""

        campaign = Campaign(
            id=UUID('8a335f6d-3d29-4ef0-a1f8-e284ba2864ac'),
            name='Test Campaign',
            user='test',
            description='Test',
            task_groups=[
                TaskGroup(
                    id=UUID('ab5454f2-83c6-4e01-848e-4d7094cb67d9'),
                    group_dependencies=[],
                    tasks=[
                        Task(
                            id=UUID('d87b1c5e-3c45-4349-a20c-f48e5096e5c5'),
                            hierarchy='1',
                            capability='cap',
                            operation_id='op_1',
                            event_name=None,
                        )
                    ],
                    objectives=[],
                )
            ],
            objectives=Objective(
                max_runtime=[
                    MaxRuntime(
                        id=UUID('f4a54745-da62-43da-8708-40d5db576b42'),
                        max_time=datetime.timedelta(minutes=10),
                        task_group=UUID('ab5454f2-83c6-4e01-848e-4d7094cb67d9'),
                    )
                ]
            ),
        )

        converter = CampaignPetriNetConverter()
        converter.convert(campaign)

        campaign_objectives = converter.get_campaign_objectives()
        assert len(campaign_objectives['max_runtimes']) == 1
        assert campaign_objectives['max_runtimes'][0]['max_time_seconds'] == 600

    def test_objectives_with_iterations(self):
        """Test extraction of iteration objectives."""

        campaign = Campaign(
            id=UUID('bd61e9df-5077-4a17-8dad-8c4b5ab1e43f'),
            name='Test Campaign',
            user='test',
            description='Test',
            task_groups=[
                TaskGroup(
                    id=UUID('e2abd536-2c37-4d97-b960-36b8f18605f2'),
                    group_dependencies=[],
                    tasks=[
                        Task(
                            id=UUID('d87b1c5e-3c45-4349-a20c-f48e5096e5c5'),
                            hierarchy='1',
                            capability='cap',
                            operation_id='op_1',
                            event_name=None,
                        )
                    ],
                    objectives=[
                        ObjectiveIterate(
                            id=UUID('920b7fd7-0a7f-4105-b7d5-79d05044a050'),
                            type='iterate',
                            iterations=5,
                        )
                    ],
                )
            ],
        )

        converter = CampaignPetriNetConverter()
        converter.convert(campaign)

        tg_objectives = converter.get_taskgroup_objectives(
            UUID('e2abd536-2c37-4d97-b960-36b8f18605f2')
        )
        assert len(tg_objectives['iterations']) == 1
        assert tg_objectives['iterations'][0]['iterations'] == 5

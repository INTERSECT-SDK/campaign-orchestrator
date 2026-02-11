"""
Tests for the campaign to Petri Net converter.
"""

import pytest
from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    TaskGroup,
    Task,
    Input,
    Output,
    Value,
)
from intersect_orchestrator.app.converters.campaign_to_petri_net import (
    CampaignPetriNetConverter,
)


@pytest.fixture
def simple_campaign() -> Campaign:
    """Create a simple campaign with two independent task groups."""
    return Campaign(
        id="test_campaign_1",
        name="Simple Campaign",
        user="test_user",
        description="A simple test campaign",
        task_groups=[
            TaskGroup(
                id="tg_1",
                group_dependencies=[],
                tasks=[
                    Task(
                        id="task_1",
                        hierarchy="1",
                        capability="capability_1",
                        operation_id="op_1",
                        event_name=None,
                    )
                ],
            ),
            TaskGroup(
                id="tg_2",
                group_dependencies=[],
                tasks=[
                    Task(
                        id="task_2",
                        hierarchy="1",
                        capability="capability_2",
                        operation_id="op_2",
                        event_name=None,
                    )
                ],
            ),
        ],
    )


@pytest.fixture
def dependent_campaign() -> Campaign:
    """Create a campaign with dependent task groups."""
    return Campaign(
        id="test_campaign_2",
        name="Dependent Campaign",
        user="test_user",
        description="A campaign with task group dependencies",
        task_groups=[
            TaskGroup(
                id="tg_setup",
                group_dependencies=[],
                tasks=[
                    Task(
                        id="task_setup",
                        hierarchy="1",
                        capability="setup",
                        operation_id="op_setup",
                        event_name=None,
                    )
                ],
            ),
            TaskGroup(
                id="tg_process",
                group_dependencies=["tg_setup"],
                tasks=[
                    Task(
                        id="task_process",
                        hierarchy="1",
                        capability="process",
                        operation_id="op_process",
                        event_name=None,
                    )
                ],
            ),
            TaskGroup(
                id="tg_finalize",
                group_dependencies=["tg_process"],
                tasks=[
                    Task(
                        id="task_finalize",
                        hierarchy="1",
                        capability="finalize",
                        operation_id="op_finalize",
                        event_name=None,
                    )
                ],
            ),
        ],
    )


@pytest.fixture
def circular_dependency_campaign() -> Campaign:
    """Create a campaign with circular dependencies (invalid)."""
    return Campaign(
        id="test_campaign_circular",
        name="Circular Campaign",
        user="test_user",
        description="A campaign with circular dependencies",
        task_groups=[
            TaskGroup(
                id="tg_a",
                group_dependencies=["tg_b"],
                tasks=[],
            ),
            TaskGroup(
                id="tg_b",
                group_dependencies=["tg_c"],
                tasks=[],
            ),
            TaskGroup(
                id="tg_c",
                group_dependencies=["tg_a"],
                tasks=[],
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

        # Verify net was created
        assert net is not None
        assert net.name == "Campaign_test_campaign_1"

        # Verify places were created
        expected_places = {
            "Ready",
            "tg_1_pending",
            "tg_1_running",
            "tg_1_complete",
            "tg_2_pending",
            "tg_2_running",
            "tg_2_complete",
            "Complete",
        }
        actual_places = {p.name for p in net.place()}
        assert actual_places == expected_places

        # Verify transitions were created
        expected_transitions = {
            "activate_tg_1",
            "complete_tg_1",
            "activate_tg_2",
            "complete_tg_2",
            "finalize_campaign",
        }
        actual_transitions = {t.name for t in net.transition()}
        assert actual_transitions == expected_transitions

        # Verify initial state
        state = converter.get_current_state()
        assert state["Ready"] == 1

    def test_dependent_campaign_conversion(self, dependent_campaign: Campaign):
        """Test conversion of a campaign with dependencies."""
        converter = CampaignPetriNetConverter()
        net = converter.convert(dependent_campaign)

        assert net is not None

        # Verify places and transitions exist
        places = {p.name for p in net.place()}
        assert "Ready" in places
        assert "tg_setup_complete" in places
        assert "tg_process_pending" in places
        assert "tg_finalize_pending" in places
        assert "Complete" in places

    def test_circular_dependency_detection(self, circular_dependency_campaign: Campaign):
        """Test that circular dependencies are detected and raise ValueError."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match="Circular dependency detected"):
            converter.convert(circular_dependency_campaign)

    def test_get_enabled_transitions_initial(self, simple_campaign: Campaign):
        """Test getting enabled transitions in initial state."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        enabled = converter.get_enabled_transitions()

        # Both independent task groups should be activatable
        assert "activate_tg_1" in enabled
        assert "activate_tg_2" in enabled

    def test_get_current_state(self, simple_campaign: Campaign):
        """Test getting the current state of the Petri Net."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        state = converter.get_current_state()

        # Ready place should have 1 token
        assert state["Ready"] == 1

        # All other places should have 0 tokens
        for place_name in state:
            if place_name != "Ready":
                assert state[place_name] == 0

    def test_fire_transition(self, simple_campaign: Campaign):
        """Test firing a transition in the Petri Net."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        initial_state = converter.get_current_state()
        assert initial_state["Ready"] == 1
        assert initial_state["tg_1_pending"] == 0

        # Fire activation transition for tg_1
        converter.fire_transition("activate_tg_1")

        state_after = converter.get_current_state()
        assert state_after["Ready"] == 0
        assert state_after["tg_1_pending"] == 1

    def test_fire_disabled_transition_raises_error(self, simple_campaign: Campaign):
        """Test that firing a disabled transition raises ValueError."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        # Try to fire a completion transition that's not enabled
        with pytest.raises(ValueError, match="is not enabled"):
            converter.fire_transition("complete_tg_1")

    def test_fire_nonexistent_transition_raises_error(self, simple_campaign: Campaign):
        """Test that firing a nonexistent transition raises ValueError."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        with pytest.raises(ValueError, match="does not exist"):
            converter.fire_transition("nonexistent_transition")

    def test_messaging_config_generation(self, simple_campaign: Campaign):
        """Test generating messaging configuration from the converter."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        config = converter.get_messaging_config()

        # Should have config for both task groups (activate + complete) + finalize
        assert "activate_tg_1" in config
        assert "complete_tg_1" in config
        assert "activate_tg_2" in config
        assert "complete_tg_2" in config
        assert "finalize_campaign" in config

        # Check topic naming
        assert (
            config["activate_tg_1"]["publish_topic"]
            == "campaign/test_campaign_1/task_group/tg_1/start"
        )
        assert (
            config["complete_tg_1"]["subscribe_topic"]
            == "campaign/test_campaign_1/task_group/tg_1/completed"
        )

    def test_workflow_execution_simple(self, simple_campaign: Campaign):
        """Test a simple workflow execution through the Petri Net."""
        converter = CampaignPetriNetConverter()
        converter.convert(simple_campaign)

        # Initial state: Ready has 1 token
        assert converter.get_current_state()["Ready"] == 1

        # Activate first task group
        converter.fire_transition("activate_tg_1")
        assert converter.get_current_state()["tg_1_pending"] == 1

        # Complete first task group
        converter.fire_transition("complete_tg_1")
        assert converter.get_current_state()["tg_1_complete"] == 1

        # Activate second task group
        converter.fire_transition("activate_tg_2")
        assert converter.get_current_state()["tg_2_pending"] == 1

        # Complete second task group
        converter.fire_transition("complete_tg_2")
        assert converter.get_current_state()["tg_2_complete"] == 1

        # Finalize campaign
        converter.fire_transition("finalize_campaign")
        assert converter.get_current_state()["Complete"] == 1

    def test_workflow_execution_with_dependencies(self, dependent_campaign: Campaign):
        """Test workflow execution with task group dependencies."""
        converter = CampaignPetriNetConverter()
        converter.convert(dependent_campaign)

        # Only setup should be initially enabled
        enabled = converter.get_enabled_transitions()
        assert "activate_tg_setup" in enabled
        assert "activate_tg_process" not in enabled

        # Activate and complete setup
        converter.fire_transition("activate_tg_setup")
        converter.fire_transition("complete_tg_setup")

        # Now process should be enabled
        enabled = converter.get_enabled_transitions()
        assert "activate_tg_process" in enabled

        # Activate and complete process
        converter.fire_transition("activate_tg_process")
        converter.fire_transition("complete_tg_process")

        # Now finalize should be enabled
        enabled = converter.get_enabled_transitions()
        assert "activate_tg_finalize" in enabled

        # Activate and complete finalize
        converter.fire_transition("activate_tg_finalize")
        converter.fire_transition("complete_tg_finalize")

        # Now finalize_campaign should be enabled
        enabled = converter.get_enabled_transitions()
        assert "finalize_campaign" in enabled

        converter.fire_transition("finalize_campaign")
        assert converter.get_current_state()["Complete"] == 1

    def test_error_on_get_state_before_conversion(self):
        """Test that getting state before conversion raises error."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match="Petri Net not created"):
            converter.get_current_state()

    def test_error_on_fire_transition_before_conversion(self):
        """Test that firing transition before conversion raises error."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match="Petri Net not created"):
            converter.fire_transition("any_transition")

    def test_error_on_messaging_config_before_conversion(self):
        """Test that getting messaging config before conversion raises error."""
        converter = CampaignPetriNetConverter()

        with pytest.raises(ValueError, match="Campaign not set"):
            converter.get_messaging_config()

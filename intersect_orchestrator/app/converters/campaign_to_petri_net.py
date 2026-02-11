"""
Converter for mapping Campaign models to Petri Nets.

This module provides conversion from the Campaign data model to snakes.nets.PetriNet,
enabling state tracking and workflow management of campaigns using Petri Net theory.
"""

from typing import Optional, Dict, Set, Tuple
from snakes.nets import PetriNet, Place, Transition, Value

from ..api.v1.endpoints.orchestrator.models.campaign import Campaign, TaskGroup, Task


class CampaignPetriNetConverter:
    """Converts Campaign models to Petri Nets for state management and execution tracking.
    
    The converter creates a Petri Net where:
    - Places represent states (Ready, TaskGroup execution states, Complete)
    - Transitions represent task groups and transitions between states
    - Tokens flow through the net based on task group execution and dependencies
    - Arc labels track which task groups must complete before others can start
    
    Example:
        >>> campaign = Campaign(...)
        >>> converter = CampaignPetriNetConverter()
        >>> petri_net = converter.convert(campaign)
    """

    def __init__(self):
        """Initialize the converter."""
        self.net: Optional[PetriNet] = None
        self.campaign: Optional[Campaign] = None
        self.task_group_map: Dict[str, TaskGroup] = {}
        self.transition_map: Dict[str, str] = {}  # Maps task group id to transition name
        self.places_created: Set[str] = set()

    def convert(self, campaign: Campaign) -> PetriNet:
        """Convert a Campaign to a Petri Net.
        
        Args:
            campaign: The Campaign model to convert
            
        Returns:
            PetriNet: A Petri Net representing the campaign's workflow
            
        Raises:
            ValueError: If the campaign has invalid structure or circular dependencies
        """
        self.campaign = campaign
        self.task_group_map = {tg.id: tg for tg in campaign.task_groups}
        
        # Create the net
        self.net = PetriNet(f"Campaign_{campaign.id}")
        
        # Validate and build the net
        self._validate_campaign()
        self._create_places()
        self._create_transitions()
        self._create_arcs()
        self._initialize_tokens()
        
        return self.net

    def _validate_campaign(self) -> None:
        """Validate campaign structure for circular dependencies and consistency.
        
        Raises:
            ValueError: If circular dependencies are detected in task groups
        """
        # Check for circular dependencies in task groups
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        
        def has_cycle(task_group_id: str) -> bool:
            visited.add(task_group_id)
            rec_stack.add(task_group_id)
            
            tg = self.task_group_map.get(task_group_id)
            if not tg:
                return False
            
            for dep_id in tg.group_dependencies:
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True
            
            rec_stack.remove(task_group_id)
            return False
        
        for tg_id in self.task_group_map.keys():
            if tg_id not in visited:
                if has_cycle(tg_id):
                    raise ValueError(
                        f"Circular dependency detected in task groups involving {tg_id}"
                    )

    def _create_places(self) -> None:
        """Create places in the Petri Net.
        
        Creates:
        - 'Ready': Initial state
        - Per task group states (e.g., 'tg_{id}_pending', 'tg_{id}_running', 'tg_{id}_complete')
        - 'Complete': Final state
        """
        # Initial place
        self.net.add_place(Place("Ready"))
        self.places_created.add("Ready")
        
        # Per-task-group places for tracking state
        for tg_id in self.task_group_map.keys():
            # Pending: waiting for dependencies to complete
            pending_place = f"tg_{tg_id}_pending"
            self.net.add_place(Place(pending_place))
            self.places_created.add(pending_place)
            
            # Running: task group is executing
            running_place = f"tg_{tg_id}_running"
            self.net.add_place(Place(running_place))
            self.places_created.add(running_place)
            
            # Complete: task group finished
            complete_place = f"tg_{tg_id}_complete"
            self.net.add_place(Place(complete_place))
            self.places_created.add(complete_place)
        
        # Final state
        self.net.add_place(Place("Complete"))
        self.places_created.add("Complete")

    def _create_transitions(self) -> None:
        """Create transitions in the Petri Net.
        
        Creates transitions for:
        - Activating task groups (when dependencies are met)
        - Completing task groups
        - Finalizing the campaign
        """
        # Transition to activate each task group
        for tg_id in self.task_group_map.keys():
            activate_trans = f"activate_{tg_id}"
            self.net.add_transition(Transition(activate_trans))
            self.transition_map[f"{tg_id}_activate"] = activate_trans
            
            # Transition to mark task group as complete
            complete_trans = f"complete_{tg_id}"
            self.net.add_transition(Transition(complete_trans))
            self.transition_map[f"{tg_id}_complete"] = complete_trans
        
        # Final transition to complete the campaign
        finalize_trans = "finalize_campaign"
        self.net.add_transition(Transition(finalize_trans))
        self.transition_map["finalize"] = finalize_trans

    def _create_arcs(self) -> None:
        """Create arcs connecting places and transitions.
        
        Handles:
        - Initial activation of independent task groups from 'Ready'
        - Dependency-based activation (task groups wait for their dependencies)
        - Completion flow from task group complete to next task groups
        - Final completion arc to 'Complete' place
        """
        # Identify independent task groups (no dependencies)
        independent_tgs = {
            tg_id for tg_id in self.task_group_map.keys()
            if not self.task_group_map[tg_id].group_dependencies
        }
        
        # For each task group, create the activation and completion flow
        for tg_id, tg in self.task_group_map.items():
            activate_trans = self.transition_map[f"{tg_id}_activate"]
            complete_trans = self.transition_map[f"{tg_id}_complete"]
            
            pending_place = f"tg_{tg_id}_pending"
            running_place = f"tg_{tg_id}_running"
            complete_place = f"tg_{tg_id}_complete"
            
            # Handle activation inputs
            if tg_id in independent_tgs:
                # Independent task groups: activate from Ready
                self.net.add_input("Ready", activate_trans, Value(1))
            else:
                # Dependent task groups: wait for all dependencies to complete
                for dep_id in tg.group_dependencies:
                    dep_complete_place = f"tg_{dep_id}_complete"
                    self.net.add_input(dep_complete_place, activate_trans, Value(1))
            
            # Activation output: token to pending place
            self.net.add_output(pending_place, activate_trans, Value(1))
            
            # Completion inputs: from pending place
            self.net.add_input(pending_place, complete_trans, Value(1))
            
            # Completion output: token to complete place
            self.net.add_output(complete_place, complete_trans, Value(1))
        
        # Finalize transition: all task groups must be complete
        finalize_trans = self.transition_map["finalize"]
        for tg_id in self.task_group_map.keys():
            complete_place = f"tg_{tg_id}_complete"
            self.net.add_input(complete_place, finalize_trans, Value(1))
        
        # Finalize output: token to final Complete place
        self.net.add_output("Complete", finalize_trans, Value(1))

    def _initialize_tokens(self) -> None:
        """Initialize the Petri Net with tokens.
        
        Adds one token to the 'Ready' place to start the workflow.
        """
        ready_place = self.net.place("Ready")
        ready_place.add(1)

    def get_messaging_config(self) -> Dict[str, Dict[str, str]]:
        """Generate messaging configuration from the Petri Net.
        
        Creates a mapping of transitions to messaging topics for external communication.
        This allows the workflow engine to publish/subscribe to messages about
        task group state changes.
        
        Returns:
            Dict mapping transition names to messaging configuration with:
            - 'publish_topic': Topic for publishing task group start events
            - 'subscribe_topic': Topic for subscribing to task group completion events
        """
        if not self.campaign:
            raise ValueError("Campaign not set. Call convert() first.")
        
        messaging_config: Dict[str, Dict[str, str]] = {}
        
        for tg_id, tg in self.task_group_map.items():
            campaign_id = self.campaign.id
            
            # Config for activation transition
            activate_trans = f"activate_{tg_id}"
            messaging_config[activate_trans] = {
                "publish_topic": f"campaign/{campaign_id}/task_group/{tg_id}/start",
                "subscribe_topic": f"campaign/{campaign_id}/task_group/{tg_id}/started",
            }
            
            # Config for completion transition
            complete_trans = f"complete_{tg_id}"
            messaging_config[complete_trans] = {
                "publish_topic": f"campaign/{campaign_id}/task_group/{tg_id}/complete",
                "subscribe_topic": f"campaign/{campaign_id}/task_group/{tg_id}/completed",
            }
        
        # Config for finalize transition
        finalize_trans = self.transition_map["finalize"]
        messaging_config[finalize_trans] = {
            "publish_topic": f"campaign/{self.campaign.id}/finalize",
            "subscribe_topic": f"campaign/{self.campaign.id}/finalized",
        }
        
        return messaging_config

    def get_enabled_transitions(self) -> list[str]:
        """Get list of currently enabled transitions in the Petri Net.
        
        Returns:
            List of transition names that are currently enabled
        """
        if not self.net:
            raise ValueError("Petri Net not created. Call convert() first.")
        
        return [t.name for t in self.net.transition() if t.enabled(self.net)]

    def get_current_state(self) -> Dict[str, int]:
        """Get the current state (marking) of the Petri Net.
        
        Returns:
            Dictionary mapping place names to number of tokens
        """
        if not self.net:
            raise ValueError("Petri Net not created. Call convert() first.")
        
        return {p.name: len(list(p.tokens)) for p in self.net.place()}

    def fire_transition(self, transition_name: str) -> None:
        """Fire a transition in the Petri Net, advancing the workflow.
        
        Args:
            transition_name: The name of the transition to fire
            
        Raises:
            ValueError: If transition is not enabled or doesn't exist
        """
        if not self.net:
            raise ValueError("Petri Net not created. Call convert() first.")
        
        try:
            trans = self.net.transition(transition_name)
            if not trans.enabled(self.net):
                raise ValueError(f"Transition '{transition_name}' is not enabled")
            trans.fire(self.net)
        except KeyError:
            raise ValueError(f"Transition '{transition_name}' does not exist in the Petri Net")

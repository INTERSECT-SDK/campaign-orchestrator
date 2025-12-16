import uuid
from datetime import datetime
from typing import Any

from ..api.v1.endpoints.orchestrator.models.icmp import Icmp
from ..api.v1.endpoints.orchestrator.models.campaign import (
    Campaign,
    Task,
    TaskGroup,
    Input,
    Output,
    Value,
)


def icmp_to_petri_net(icmp_data: dict[str, Any]) -> dict[str, Any]:
    """Convert ICMP campaign JSON to Petri net YAML format.

    Args:
        icmp_data: The ICMP campaign data as a dictionary

    Returns:
        Petri net representation as a dictionary
    """
    nodes = icmp_data.get('nodes', [])
    edges = icmp_data.get('edges', [])

    # Generate net name based on node types and count
    if len(nodes) == 1:
        node = nodes[0]
        if node.get('type') == 'capability':
            capability_name = node.get('data', {}).get('capability', {}).get('name', '')
            if 'Random_Number' in capability_name:
                net_name = 'RandomNumberWorkflow'
            else:
                net_name = f'{capability_name}Workflow'
        else:
            net_name = 'SingleNodeWorkflow'
    elif len(nodes) == 2:
        # Check if it has visualization
        has_viz = any(node.get('type') == 'visualization' for node in nodes)
        if has_viz:
            net_name = 'RandomNumberAndHistogramWorkflow'
        else:
            net_name = 'MultiNodeWorkflow'
    else:
        net_name = f'{len(nodes)}NodeWorkflow'

    # Create places based on workflow states
    places = ['Ready', 'Complete']

    # Add intermediate places based on edges
    if edges and len(nodes) > 1:
        places.insert(1, 'Processing')

    transitions = []

    if not nodes:
        # Empty workflow
        transitions.append(
            {
                'name': 'EmptyWorkflow',
                'inputs': ['Ready'],
                'outputs': ['Complete'],
                'publish_topic': 'workflow/empty/start',
                'subscribe_topic': 'workflow/empty/done',
            }
        )
    elif len(nodes) == 1:
        # Single node workflow
        node = nodes[0]
        node_name = _extract_node_name(node)
        transitions.append(
            {
                'name': node_name,
                'inputs': ['Ready'],
                'outputs': ['Complete'],
                'publish_topic': f'{node_name.lower().replace(" ", "")}/start',
                'subscribe_topic': f'{node_name.lower().replace(" ", "")}/done',
            }
        )
    else:
        # Multi-node workflow
        for i, node in enumerate(nodes):
            node_name = _extract_node_name(node)
            if i == 0:
                # First node
                inputs = ['Ready']
                outputs = ['Processing']
            elif i == len(nodes) - 1:
                # Last node
                inputs = ['Processing']
                outputs = ['Complete']
            else:
                # Intermediate node
                inputs = ['Processing']
                outputs = ['Processing']

            transitions.append(
                {
                    'name': node_name,
                    'inputs': inputs,
                    'outputs': outputs,
                    'publish_topic': f'{node_name.lower().replace(" ", "")}/start',
                    'subscribe_topic': f'{node_name.lower().replace(" ", "")}/done',
                }
            )

    return {'net_name': net_name, 'places': places, 'transitions': transitions}


def petri_net_to_icmp(petri_net: dict[str, Any]) -> dict[str, Any]:
    """Convert Petri net YAML to ICMP campaign JSON format.

    Args:
        petri_net: The Petri net data as a dictionary

    Returns:
        ICMP campaign representation as a dictionary
    """
    net_name = petri_net.get('net_name', 'ConvertedWorkflow')
    transitions = petri_net.get('transitions', [])

    nodes = []
    edges = []

    # Create nodes from transitions
    for i, transition in enumerate(transitions):
        node_id = str(uuid.uuid4())
        node = {
            'id': node_id,
            'type': 'capability',
            'data': {
                'capability': {
                    'name': transition['name'],
                    'created_at': datetime.now().isoformat(),
                    'last_lifecycle_message': datetime.now().isoformat(),
                    'service_id': i + 1,
                    'endpoints_schema': {
                        'channels': {
                            transition['name'].lower(): {
                                'publish': {
                                    'message': {
                                        'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                                        'contentType': 'application/json',
                                        'traits': {
                                            '$ref': '#/components/messageTraits/commonHeaders'
                                        },
                                        'payload': {'type': 'object'},
                                    },
                                    'description': f'Execute {transition["name"]}',
                                },
                                'subscribe': {
                                    'message': {
                                        'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                                        'contentType': 'application/json',
                                        'traits': {
                                            '$ref': '#/components/messageTraits/commonHeaders'
                                        },
                                        'payload': {'type': 'object'},
                                    },
                                    'description': f'Complete {transition["name"]}',
                                },
                                'events': [],
                            }
                        }
                    },
                },
                'endpoint': transition['name'].lower(),
                'endpoint_channel': {
                    'publish': {
                        'message': {
                            'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                            'contentType': 'application/json',
                            'traits': {'$ref': '#/components/messageTraits/commonHeaders'},
                            'payload': {'type': 'object'},
                        },
                        'description': f'Execute {transition["name"]}',
                    },
                    'subscribe': {
                        'message': {
                            'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                            'contentType': 'application/json',
                            'traits': {'$ref': '#/components/messageTraits/commonHeaders'},
                            'payload': {'type': 'object'},
                        },
                        'description': f'Complete {transition["name"]}',
                    },
                    'events': [],
                },
            },
            'position': {'x': i * 200, 'y': 0},
            'measured': {'width': 200, 'height': 100},
            'selected': False,
            'dragging': False,
        }
        nodes.append(node)

    # Create edges based on transition connections
    for i in range(len(transitions) - 1):
        source_node = nodes[i]
        target_node = nodes[i + 1]

        edge = {
            'id': f'edge-{source_node["id"]}-to-{target_node["id"]}',
            'source': source_node['id'],
            'target': target_node['id'],
            'sourceHandle': 'output-right',
            'targetHandle': 'input-left',
            'type': 'baseCampaignEdge',
            'markerEnd': {'type': 'arrowclosed', 'width': 20, 'height': 20, 'color': '#000000'},
        }
        edges.append(edge)

    return {
        'campaignId': f'Campaign-{uuid.uuid4().hex[:8]}.icmp',
        'campaignName': net_name,
        'nodes': nodes,
        'edges': edges,
        'createdAt': datetime.now().isoformat() + 'Z',
        'updatedAt': datetime.now().isoformat() + 'Z',
    }


def _extract_node_name(node: dict[str, Any]) -> str:
    """Extract a human-readable name from an ICMP node."""
    if node.get('type') == 'capability':
        capability_data = node.get('data', {}).get('capability', {})
        name = capability_data.get('name', f'Node_{node["id"][:8]}')
        return name.replace('_', '').replace(' ', '')
    if node.get('type') == 'visualization':
        viz_data = node.get('data', {})
        name = viz_data.get('name', f'Visualization_{node["id"][:8]}')
        return name.replace(' ', '')
    return f'Node_{node["id"][:8]}'


def icmp_to_campaign(icmp_data: dict[str, Any]) -> 'Campaign':
    """Convert ICMP JSON data to Campaign model.

    Args:
        icmp_data: The ICMP JSON data as a dictionary

    Returns:
        Campaign model instance
    """

    # Extract basic campaign info
    campaign_id = icmp_data.get('campaignId', str(uuid.uuid4()))
    campaign_name = icmp_data.get('campaignName', 'Converted Campaign')

    # Create Icmp model from data to validate and access typed data
    icmp = Icmp(**icmp_data)

    # Group nodes by type
    capability_nodes = []
    visualization_nodes = []

    for node in icmp.nodes:
        if hasattr(node, 'type'):
            if node.type == 'capability':
                capability_nodes.append(node)
            elif node.type == 'visualization':
                visualization_nodes.append(node)

    # Create task groups
    task_groups = []

    # Create capability task group
    if capability_nodes:
        capability_tasks = []
        for node in capability_nodes:
            if hasattr(node.data, 'capability') and hasattr(node.data, 'endpoint'):
                # Create input schema from endpoint channel subscribe
                input_schema = {
                    'type': 'object',
                    'properties': {
                        'seed': {
                            'type': 'integer',
                            'minimum': 0,
                            'default': 0,
                            'description': 'Random number generator seed.',
                        }
                    },
                }

                # Create output schema from endpoint channel publish
                output_schema = {
                    'type': 'object',
                    'properties': {
                        'random_number': {
                            'type': 'number',
                            'description': 'Generated random number.',
                        }
                    },
                }

                task = Task(
                    id=node.data.endpoint,
                    hierarchy='capability',
                    capability=node.data.capability.name,
                    operation_id=node.data.endpoint,
                    input=Input(
                        schema=input_schema,
                        values=[Value(id='seed_input', var='seed', type='integer', value=0)],
                    ),
                    output=Output(
                        schema=output_schema,
                        values=[
                            Value(id='random_output', var='random_number', type='number', value=0.0)
                        ],
                    ),
                )
                capability_tasks.append(task)

        capability_group = TaskGroup(id='capability_group', tasks=capability_tasks)
        task_groups.append(capability_group)

    # Create visualization task group with dependency on capability group
    if visualization_nodes:
        visualization_tasks = []
        for node in visualization_nodes:
            if hasattr(node.data, 'type') and hasattr(node.data, 'name'):
                task = Task(
                    id=f'{node.data.type}_{node.data.name.lower().replace(" ", "_")}',
                    hierarchy='visualization',
                    capability='Visualization',
                    operation_id=f'{node.data.type}_{node.data.name.lower().replace(" ", "_")}',
                    input=None,
                    output=None,
                )
                visualization_tasks.append(task)

        visualization_group = TaskGroup(
            id='visualization_group',
            group_dependencies=['capability_group'] if capability_nodes else [],
            tasks=visualization_tasks,
        )
        task_groups.append(visualization_group)

    # Create campaign
    campaign = Campaign(
        id=campaign_id,
        name=campaign_name,
        user='system',  # Default user
        description=f'Campaign converted from ICMP: {campaign_name}',
        task_groups=task_groups,
    )

    return campaign


def campaign_to_icmp(campaign: Campaign) -> dict[str, Any]:
    """Convert Campaign model to ICMP JSON format.

    Args:
        campaign: The Campaign model instance

    Returns:
        ICMP JSON data as a dictionary
    """
    nodes = []
    edges = []

    # Process each task group
    for task_group in campaign.task_groups:
        for task in task_group.tasks:
            if task.hierarchy == 'capability':
                # Create capability node
                capability_node = {
                    'id': str(uuid.uuid4()),
                    'type': 'capability',
                    'data': {
                        'capability': {
                            'name': task.capability,
                            'created_at': datetime.now().isoformat(),
                            'last_lifecycle_message': None,
                            'service_id': 1,  # Default service ID
                            'endpoints_schema': {
                                'channels': {
                                    task.operation_id: {
                                        'publish': {
                                            'message': {
                                                'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                                                'contentType': 'application/json',
                                                'traits': {
                                                    '$ref': '#/components/messageTraits/commonHeaders'
                                                },
                                                'payload': {
                                                    'type': 'object',
                                                    'properties': {
                                                        'random_number': {
                                                            'type': 'number',
                                                            'description': 'Generated random number.',
                                                        }
                                                    },
                                                },
                                            },
                                            'description': f'Execute {task.operation_id}',
                                        },
                                        'subscribe': {
                                            'message': {
                                                'schemaFormat': 'application/vnd.aai.asyncapi+json;version=2.6.0',
                                                'contentType': 'application/json',
                                                'traits': {
                                                    '$ref': '#/components/messageTraits/commonHeaders'
                                                },
                                                'payload': {
                                                    'default': 0,
                                                    'description': f'Input for {task.operation_id}',
                                                    'minimum': 0,
                                                    'title': 'input',
                                                    'type': 'integer',
                                                },
                                            },
                                            'description': f'Execute {task.operation_id}',
                                        },
                                        'events': [],
                                    }
                                }
                            },
                        },
                        'endpoint': task.operation_id,
                        'endpoint_channel': {},
                    },
                }
                nodes.append(capability_node)

            elif task.hierarchy == 'visualization':
                # Create visualization node
                viz_type = (
                    task.operation_id.split('_')[0] if '_' in task.operation_id else 'unknown'
                )
                viz_name = (
                    '_'.join(task.operation_id.split('_')[1:])
                    if '_' in task.operation_id
                    else task.operation_id
                )

                visualization_node = {
                    'id': str(uuid.uuid4()),
                    'type': 'visualization',
                    'data': {
                        'type': viz_type,
                        'name': viz_name.replace('_', ' ').title(),
                        'spec': {},  # Empty spec for now
                    },
                }
                nodes.append(visualization_node)

    # Create edges based on task group dependencies
    node_id_map = {}

    for node in nodes:
        if node['type'] == 'capability':
            # Key is the endpoint (matches operation_id)
            key = node['data']['endpoint']
        elif node['type'] == 'visualization':
            # Key is the operation_id format used in tasks
            viz_type = node['data']['type']
            viz_name = node['data']['name'].lower().replace(' ', '_')
            key = f'{viz_type}_{viz_name}'
        else:
            key = node['id']  # Fallback

        node_id_map[key] = node['id']

    for task_group in campaign.task_groups:
        if task_group.group_dependencies:
            for dep_group_id in task_group.group_dependencies:
                # Find tasks in dependent group
                dep_group = next((tg for tg in campaign.task_groups if tg.id == dep_group_id), None)
                if dep_group:
                    for dep_task in dep_group.tasks:
                        for current_task in task_group.tasks:
                            # Create edge from dependency task output to current task input
                            dep_node_id = node_id_map.get(dep_task.operation_id)
                            current_node_id = node_id_map.get(current_task.operation_id)

                            if dep_node_id and current_node_id:
                                edge = {
                                    'id': str(uuid.uuid4()),
                                    'source': dep_node_id,
                                    'target': current_node_id,
                                    'sourceHandle': 'output-right',
                                    'targetHandle': 'input-left',
                                    'type': 'baseCampaignEdge',
                                    'markerEnd': {
                                        'type': 'arrowclosed',
                                        'width': 20,
                                        'height': 20,
                                        'color': '#000000',
                                    },
                                }
                                edges.append(edge)

    # Create ICMP data structure
    icmp_data = {
        'campaignId': campaign.id,
        'campaignName': campaign.name,
        'nodes': nodes,
        'edges': edges,
        'createdAt': datetime.now().isoformat() + 'Z',
        'updatedAt': datetime.now().isoformat() + 'Z',
    }

    return icmp_data

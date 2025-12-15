import uuid
from datetime import datetime
from typing import Any


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

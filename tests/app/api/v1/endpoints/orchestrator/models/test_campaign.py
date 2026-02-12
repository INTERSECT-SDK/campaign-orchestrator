from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign


def test_random_number_campaign_data(random_number_campaign_data):
    """Validate the random number campaign fixture against the Campaign model."""
    campaign = Campaign(**random_number_campaign_data)

    assert campaign.id == random_number_campaign_data['id']
    assert campaign.name == random_number_campaign_data['name']
    assert len(campaign.task_groups) == 1

    task_group = campaign.task_groups[0]
    assert task_group.id == 'capability_group'
    assert len(task_group.tasks) == 2
    assert task_group.tasks[0].id == 'generate_random_number'
    assert task_group.tasks[1].id == 'validate_random_number'
    assert task_group.tasks[1].task_dependencies == ['generate_random_number']


def test_random_number_and_histogram_campaign_data(random_number_and_histogram_campaign_data):
    """Validate the random number + histogram campaign fixture."""
    campaign = Campaign(**random_number_and_histogram_campaign_data)

    assert campaign.id == random_number_and_histogram_campaign_data['id']
    assert campaign.name == random_number_and_histogram_campaign_data['name']
    assert len(campaign.task_groups) == 2

    task_groups = {tg.id: tg for tg in campaign.task_groups}
    assert 'capability_group' in task_groups
    assert 'visualization_group' in task_groups

    viz_group = task_groups['visualization_group']
    assert 'capability_group' in viz_group.group_dependencies

    capability_group = task_groups['capability_group']
    assert len(capability_group.tasks) == 2
    assert capability_group.tasks[1].task_dependencies == ['generate_random_number']

    assert len(viz_group.tasks) == 2
    assert viz_group.tasks[1].task_dependencies == ['vega-histogram_histogram']

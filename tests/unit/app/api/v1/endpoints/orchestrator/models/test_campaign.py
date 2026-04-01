from uuid import UUID

from intersect_orchestrator.app.api.v1.endpoints.orchestrator.models.campaign import Campaign


def test_random_number_campaign_data(random_number_campaign_data):
    """Validate the random number campaign fixture against the Campaign model."""
    campaign = Campaign(**random_number_campaign_data)

    assert str(campaign.id) == random_number_campaign_data['id']
    assert campaign.name == random_number_campaign_data['name']
    assert len(campaign.task_groups) == 1

    task_group = campaign.task_groups[0]
    assert str(task_group.id) == '6726b2ca-8d25-4be9-bb34-6ec95a090d56'
    assert len(task_group.tasks) == 2
    assert str(task_group.tasks[0].id) == '0434b32a-f7c9-4a10-908a-aff0b5daa696'
    assert str(task_group.tasks[1].id) == 'bf0c4877-14f3-4c88-9019-503f4c10dd90'
    assert [str(dep) for dep in task_group.tasks[1].task_dependencies] == [
        '0434b32a-f7c9-4a10-908a-aff0b5daa696'
    ]


def test_random_number_and_histogram_campaign_data(random_number_and_histogram_campaign_data):
    """Validate the random number + histogram campaign fixture."""
    campaign = Campaign(**random_number_and_histogram_campaign_data)

    assert str(campaign.id) == random_number_and_histogram_campaign_data['id']
    assert campaign.name == random_number_and_histogram_campaign_data['name']
    assert len(campaign.task_groups) == 2

    uuid_1 = UUID('5bb1cb46-f541-4517-a544-40f2ae18a7e7')
    uuid_2 = UUID('475e09fa-eaac-4cd1-8d45-22bc2580befd')

    task_groups = {tg.id: tg for tg in campaign.task_groups}
    assert uuid_1 in task_groups
    assert uuid_2 in task_groups

    viz_group = task_groups[uuid_2]
    assert uuid_1 in viz_group.group_dependencies

    capability_group = task_groups[uuid_1]
    assert len(capability_group.tasks) == 2
    assert capability_group.tasks[1].task_dependencies == [
        UUID('754bfbb8-2b81-422b-9906-2b291ac77a33')
    ]

    assert len(viz_group.tasks) == 2
    assert viz_group.tasks[1].task_dependencies == [UUID('93b5f409-ae29-413d-96a9-b173d0265f25')]

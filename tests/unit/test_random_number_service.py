import pytest

pytest.importorskip('intersect_sdk')


from services.random_number_service.random_number_generator import (
    GenerateRandomNumberRequest,
    RandomServiceRandomNumGenCapabilityImpl,
)


def test_generate_random_number_returns_stream_without_reseeding() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    first = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value
    second = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value
    third = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value

    assert [first, second, third] == [50, 98, 54]


def test_generate_random_number_keeps_streams_independent() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    x_first = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value
    y_first = service.generate_random_number(GenerateRandomNumberRequest(seed=1, stream_id='y')).value
    x_second = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value
    y_second = service.generate_random_number(GenerateRandomNumberRequest(seed=1, stream_id='y')).value

    assert [x_first, x_second] == [50, 98]
    assert [y_first, y_second] == [18, 73]


def test_reset_restores_deterministic_stream() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x'))
    service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x'))

    response = service.reset()
    assert response.state.streams == {}

    first_after_reset = service.generate_random_number(
        GenerateRandomNumberRequest(seed=0, stream_id='x')
    ).value
    second_after_reset = service.generate_random_number(
        GenerateRandomNumberRequest(seed=0, stream_id='x')
    ).value

    assert [first_after_reset, second_after_reset] == [50, 98]

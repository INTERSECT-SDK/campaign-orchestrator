import pytest


pytest.importorskip('intersect_sdk')


from services.random_number_service.random_number_generator import (
    RandomServiceRandomNumGenCapabilityImpl,
)


def test_generate_random_number_returns_stream_without_reseeding() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    first = service.generate_random_number().state.numbers[-1]
    second = service.generate_random_number().state.numbers[-1]
    third = service.generate_random_number().state.numbers[-1]

    assert [first, second, third] == [50, 98, 54]


def test_reset_restores_deterministic_stream() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    service.generate_random_number()
    service.generate_random_number()

    response = service.reset()
    assert response.state.numbers == []

    first_after_reset = service.generate_random_number().state.numbers[-1]
    second_after_reset = service.generate_random_number().state.numbers[-1]

    assert [first_after_reset, second_after_reset] == [50, 98]
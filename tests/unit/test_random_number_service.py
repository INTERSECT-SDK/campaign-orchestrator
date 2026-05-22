import pytest

pytest.importorskip('intersect_sdk')


from services.random_number_service.random_number_generator import (
    GenerateRandomNumberRequest,
    RandomServiceRandomNumGenCapabilityImpl,
)


def test_generate_random_number_returns_stream_without_reseeding() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    first = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value
    second = service.generate_random_number(
        GenerateRandomNumberRequest(seed=0, stream_id='x')
    ).value
    third = service.generate_random_number(GenerateRandomNumberRequest(seed=0, stream_id='x')).value

    assert [first, second, third] == [50, 98, 54]


def test_generate_random_number_keeps_streams_independent() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()

    x_first = service.generate_random_number(
        GenerateRandomNumberRequest(seed=0, stream_id='x')
    ).value
    y_first = service.generate_random_number(
        GenerateRandomNumberRequest(seed=1, stream_id='y')
    ).value
    x_second = service.generate_random_number(
        GenerateRandomNumberRequest(seed=0, stream_id='x')
    ).value
    y_second = service.generate_random_number(
        GenerateRandomNumberRequest(seed=1, stream_id='y')
    ).value

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


def test_start_measurement_emits_intersect_event() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()
    emitted: list[tuple[str, int]] = []

    def _capture(event_name: str, event_value: int) -> None:
        emitted.append((event_name, event_value))

    service.intersect_sdk_emit_event = _capture  # type: ignore[method-assign]

    response = service.start_measurement(
        GenerateRandomNumberRequest(seed=0, stream_id='event-stream')
    )

    assert emitted == [('newMeasurement', response.value)]


def test_emit_new_measurement_requires_start_measurement() -> None:
    service = RandomServiceRandomNumGenCapabilityImpl()
    emitted: list[tuple[str, int]] = []

    def _capture(event_name: str, event_value: int) -> None:
        emitted.append((event_name, event_value))

    service.intersect_sdk_emit_event = _capture  # type: ignore[method-assign]

    response = service.emit_new_measurement(
        GenerateRandomNumberRequest(seed=0, stream_id='event-stream')
    )

    assert response.success is False
    assert emitted == []

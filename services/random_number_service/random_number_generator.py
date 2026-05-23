"""Class to define Random Number Generator capability."""

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Annotated, ClassVar

from intersect_sdk import (
    HierarchyConfig,
    IntersectBaseCapabilityImplementation,
    IntersectEventDefinition,
    IntersectService,
    IntersectServiceConfig,
    default_intersect_lifecycle_loop,
    intersect_message,
    intersect_status,
)
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


DEFAULT_RANDOM_SEED = 0
MEASUREMENT_INTERVAL_SECONDS = 0.2


class RandomServiceRandomNumGenCapabilityImplState(BaseModel):
    """Model of the capability's state."""

    streams: Annotated[
        dict[str, list[int]],
        Field(description='Generated numbers keyed by logical stream id.'),
    ] = Field(default_factory=dict)
    active_measurement_streams: Annotated[
        list[str],
        Field(description='Stream IDs that will emit newMeasurement events.'),
    ] = Field(default_factory=list)


@dataclass
class RandomServiceRandomNumGenCapabilityImplResponse:
    """Model of capability response."""

    stream_id: str
    value: int
    state: RandomServiceRandomNumGenCapabilityImplState
    success: bool


class GenerateRandomNumberRequest(BaseModel):
    """Input payload for random number generation."""

    seed: Annotated[
        int | None,
        Field(
            title='seed',
            description='Optional random number generator seed. If omitted, the stream continues its current random sequence.',
            default=None,
            ge=0,
        ),
    ] = None
    stream_id: Annotated[
        str,
        Field(
            title='stream_id',
            description='Logical stream identifier (e.g. x/y) to keep RNG streams independent.',
            default='default',
            min_length=1,
        ),
    ] = 'default'
    delay: Annotated[
        float,
        Field(
            title='delay',
            description='Seconds to wait before generating the number (simulates work).',
            default=0.0,
            ge=0.0,
        ),
    ] = 0.0


class RandomServiceRandomNumGenCapabilityImpl(IntersectBaseCapabilityImplementation):
    """Capability class implementation."""

    intersect_sdk_capability_name = 'RandomNumberGenerator'
    intersect_sdk_events: ClassVar[dict[str, IntersectEventDefinition]] = {
        'newMeasurement': IntersectEventDefinition(event_type=int),
    }

    def __init__(self) -> None:
        """Constructors are never exposed to INTERSECT."""
        super().__init__()
        self.state = RandomServiceRandomNumGenCapabilityImplState()
        self._measurement_lock = threading.RLock()
        self._rng_by_stream: dict[str, random.Random] = {}
        self._stream_seed_initialized: set[str] = set()
        self._measurement_streams_started: set[str] = set()
        self._measurement_stop_events: dict[str, threading.Event] = {}
        self._measurement_threads: dict[str, threading.Thread] = {}

    @intersect_status()
    def status(self) -> RandomServiceRandomNumGenCapabilityImplState:
        """Return status of current state."""
        logger.info('Status requested, current state: %s', self.state)
        return self.state

    def _stream_worker(self, stream_id: str) -> None:
        """Continuously emit measurements while a stream is active."""
        with self._measurement_lock:
            stop_event = self._measurement_stop_events.get(stream_id)
            if stop_event is None:
                return
        while not stop_event.wait(MEASUREMENT_INTERVAL_SECONDS):
            self._generate_random_number(
                GenerateRandomNumberRequest(stream_id=stream_id),
                stop_event=stop_event,
            )

    def _generate_random_number(
        self,
        request: GenerateRandomNumberRequest,
        *,
        stop_event: threading.Event | None = None,
    ) -> RandomServiceRandomNumGenCapabilityImplResponse | None:
        """Generate a random number while holding the measurement lock."""
        seed = request.seed
        stream_id = request.stream_id

        with self._measurement_lock:
            if stop_event is not None and stop_event.is_set():
                return None

            if stream_id not in self._rng_by_stream:
                self._rng_by_stream[stream_id] = random.Random(DEFAULT_RANDOM_SEED)  # noqa: S311
                self.state.streams.setdefault(stream_id, [])

            rng = self._rng_by_stream[stream_id]

            # Seed is applied only once per stream unless reset(), so each stream
            # remains deterministic while still advancing across iterations.
            if seed is not None and stream_id not in self._stream_seed_initialized:
                rng.seed(seed)
                self._stream_seed_initialized.add(stream_id)

            random_int = rng.randint(1, 100)

            self.state.streams.setdefault(stream_id, []).append(random_int)
            should_emit_event = stream_id in self._measurement_streams_started

            response = RandomServiceRandomNumGenCapabilityImplResponse(
                stream_id=stream_id,
                value=random_int,
                state=self.state,
                success=True,
            )

        if should_emit_event:
            logger.info('Emitting newMeasurement event for stream %s value %s', stream_id, random_int)
            self.intersect_sdk_emit_event('newMeasurement', random_int)

        logger.info('Generated random number: %s for stream %s', random_int, stream_id)
        return response

    @intersect_message()
    def generate_random_number(
        self,
        request: Annotated[
            GenerateRandomNumberRequest,
            Field(default_factory=GenerateRandomNumberRequest),
        ],
    ) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Generate random number."""
        seed = request.seed
        stream_id = request.stream_id
        delay = request.delay
        logger.info('generate_random_number called with stream_id=%s seed=%s delay=%.1fs', stream_id, seed, delay)

        if delay > 0:
            logger.info('Simulating work for %.1f seconds...', delay)
            time.sleep(delay)

        response = self._generate_random_number(request)
        assert response is not None
        return response

    @intersect_message()
    def reset(self) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Reset state."""
        logger.warning('reset called')

        with self._measurement_lock:
            stop_events = tuple(self._measurement_stop_events.values())
            measurement_threads = tuple(self._measurement_threads.values())
            for stop_event in stop_events:
                stop_event.set()

        for worker in measurement_threads:
            worker.join(timeout=MEASUREMENT_INTERVAL_SECONDS)

        with self._measurement_lock:
            self.state.streams = {}
            self.state.active_measurement_streams = []
            self._rng_by_stream = {}
            self._stream_seed_initialized = set()
            self._measurement_streams_started = set()
            self._measurement_stop_events = {}
            self._measurement_threads = {}

            return RandomServiceRandomNumGenCapabilityImplResponse(
                stream_id='default',
                value=0,
                state=self.state,
                success=True,
            )

    @intersect_message()
    def start_measurement(
        self,
        request: Annotated[
            GenerateRandomNumberRequest,
            Field(default_factory=GenerateRandomNumberRequest),
        ],
    ) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Activate stream emissions and emit the first measurement immediately."""
        stream_id = request.stream_id
        with self._measurement_lock:
            if stream_id not in self._measurement_streams_started:
                self._measurement_streams_started.add(stream_id)
                stop_event = threading.Event()
                self._measurement_stop_events[stream_id] = stop_event
                worker = threading.Thread(
                    target=self._stream_worker,
                    args=(stream_id,),
                    daemon=True,
                    name=f'measurement-stream-{stream_id}',
                )
                self._measurement_threads[stream_id] = worker
                worker.start()

            self.state.active_measurement_streams = sorted(self._measurement_streams_started)
            response = self._generate_random_number(request)

        assert response is not None
        return response

    @intersect_message()
    def emit_new_measurement(
        self,
        request: Annotated[
            GenerateRandomNumberRequest,
            Field(default_factory=GenerateRandomNumberRequest),
        ],
    ) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Emit one measurement when stream output has been activated."""
        stream_id = request.stream_id
        with self._measurement_lock:
            if stream_id not in self._measurement_streams_started:
                logger.warning(
                    'emit_new_measurement called before start_measurement for stream %s', stream_id
                )
                return RandomServiceRandomNumGenCapabilityImplResponse(
                    stream_id=stream_id,
                    value=0,
                    state=self.state,
                    success=False,
                )

            response = self._generate_random_number(request)

        assert response is not None
        return response

    @staticmethod
    def run() -> None:
        from_config_file = {
            'brokers': [
                {
                    'username': os.getenv('BROKER_USERNAME'),
                    'password': os.getenv('BROKER_PASSWORD'),
                    'host': os.getenv('BROKER_HOST'),
                    'port': os.getenv('BROKER_PORT'),
                    'protocol': os.getenv('BROKER_PROTOCOL'),
                },
            ],
        }
        config = IntersectServiceConfig(
            hierarchy=HierarchyConfig(
                organization='random-organization',
                facility='random-facility',
                system='random-system',
                subsystem='random-subsystem',
                service='random-service',
            ),
            status_interval=30.0,
            **from_config_file,
        )
        capability = RandomServiceRandomNumGenCapabilityImpl()
        service = IntersectService([capability], config)
        logger.info(
            'Starting %s, use Ctrl+C to exit.',
            RandomServiceRandomNumGenCapabilityImpl.intersect_sdk_capability_name,
        )
        default_intersect_lifecycle_loop(
            service,
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    #logging.getLogger('intersect-sdk').setLevel(logging.DEBUG)
    #logging.getLogger('intersect-sdk-common').setLevel(logging.DEBUG)
    RandomServiceRandomNumGenCapabilityImpl.run()

"""Class to define Random Number Generator capability."""

import logging
import os
import random
from dataclasses import dataclass
from typing import Annotated

from intersect_sdk import (
    HierarchyConfig,
    IntersectBaseCapabilityImplementation,
    IntersectService,
    IntersectServiceConfig,
    default_intersect_lifecycle_loop,
    intersect_message,
    intersect_status,
)
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


DEFAULT_RANDOM_SEED = 0


class RandomServiceRandomNumGenCapabilityImplState(BaseModel):
    """Model of the capability's state."""

    streams: Annotated[
        dict[str, list[int]],
        Field(description='Generated numbers keyed by logical stream id.'),
    ] = Field(default_factory=dict)


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


class RandomServiceRandomNumGenCapabilityImpl(IntersectBaseCapabilityImplementation):
    """Capability class implementation."""

    intersect_sdk_capability_name = 'Random_Number_Generator'

    def __init__(self) -> None:
        """Constructors are never exposed to INTERSECT."""
        super().__init__()
        self.state = RandomServiceRandomNumGenCapabilityImplState()
        self._rng_by_stream: dict[str, random.Random] = {}
        self._stream_seed_initialized: set[str] = set()

    @intersect_status()
    def status(self) -> RandomServiceRandomNumGenCapabilityImplState:
        """Return status of current state."""
        logger.info('Status requested, current state: %s', self.state)
        return self.state

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
        logger.warning('generate_random_number called with stream_id=%s seed=%s', stream_id, seed)

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

        # Update state
        self.state.streams.setdefault(stream_id, []).append(random_int)

        logger.info('Generated random number: %s for stream %s', random_int, stream_id)

        return RandomServiceRandomNumGenCapabilityImplResponse(
            stream_id=stream_id,
            value=random_int,
            state=self.state,
            success=True,
        )

    @intersect_message()
    def reset(self) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Reset state."""
        logger.warning('reset called')

        self.state.streams = {}
        self._rng_by_stream = {}
        self._stream_seed_initialized = set()

        return RandomServiceRandomNumGenCapabilityImplResponse(
            stream_id='default',
            value=0,
            state=self.state,
            success=True,
        )

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
    logging.basicConfig(level=logging.WARNING)
    #logging.getLogger('intersect-sdk').setLevel(logging.DEBUG)
    #logging.getLogger('intersect-sdk-common').setLevel(logging.DEBUG)
    RandomServiceRandomNumGenCapabilityImpl.run()

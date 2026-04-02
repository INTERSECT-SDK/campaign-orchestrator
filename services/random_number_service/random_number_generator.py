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

    numbers: Annotated[list[int], Field(description='All generated numbers.')] = []


@dataclass
class RandomServiceRandomNumGenCapabilityImplResponse:
    """Model of capability response."""

    state: RandomServiceRandomNumGenCapabilityImplState
    success: bool


class RandomServiceRandomNumGenCapabilityImpl(IntersectBaseCapabilityImplementation):
    """Capability class implementation."""

    intersect_sdk_capability_name = 'Random_Number_Generator'

    def __init__(self) -> None:
        """Constructors are never exposed to INTERSECT."""
        super().__init__()
        self.state = RandomServiceRandomNumGenCapabilityImplState()
        self._rng = random.Random(DEFAULT_RANDOM_SEED)

    @intersect_status()
    def status(self) -> RandomServiceRandomNumGenCapabilityImplState:
        """Return status of current state."""
        logger.info('Status requested, current state: %s', self.state)
        return self.state

    @intersect_message()
    def generate_random_number(
        self,
        seed: Annotated[
            int | None,
            Field(
                title='seed',
                description='Optional random number generator seed. If omitted, the service continues its current random stream.',
                default=None,
                ge=0,
            ),
        ],
    ) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Generate random number."""
        logger.warning('generate_random_number called with seed %s', seed)
        if seed is not None:
            self._rng.seed(seed)
        random_int = self._rng.randint(1, 100)  # noqa: S311

        # Update state
        numbers = self.state.numbers
        numbers.append(random_int)
        self.state.numbers = numbers

        logger.info('Generated random number: %s', random_int)

        return RandomServiceRandomNumGenCapabilityImplResponse(
            state=self.state,
            success=True,
        )

    @intersect_message()
    def reset(self) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Reset state."""
        logger.warning('reset called')

        self.state.numbers = []
        self._rng.seed(DEFAULT_RANDOM_SEED)

        return RandomServiceRandomNumGenCapabilityImplResponse(
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

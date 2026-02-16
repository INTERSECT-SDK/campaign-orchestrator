"""Class to define Random Number Generator capability."""

import logging
import os
import random
from dataclasses import dataclass

# from typing import Optional
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    @intersect_status()
    def status(self) -> RandomServiceRandomNumGenCapabilityImplState:
        """Return status of current state."""
        return self.state

    @intersect_message()
    def generate_random_number(
        self,
        seed: Annotated[
            int,
            Field(
                title='seed',
                description='Random number generator seed.',
                default=0,
                ge=0,
            ),
        ],
    ) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Generate random number."""
        random.seed(seed)
        random_int = random.randint(1, 100)  # noqa: S311

        # Update state
        numbers = self.state.numbers
        numbers.append(random_int)
        self.state.numbers = numbers

        return RandomServiceRandomNumGenCapabilityImplResponse(
            state=self.state,
            success=True,
        )

    @intersect_message()
    def reset(self) -> RandomServiceRandomNumGenCapabilityImplResponse:
        """Reset state."""
        self.state.numbers = []

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
    RandomServiceRandomNumGenCapabilityImpl.run()

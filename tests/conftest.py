import asyncio
from unittest import mock

import pytest

from cqrs.adapters import kafka

TEST_TOPIC = "TestCqrsTopic"

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    try:
        yield loop
    finally:
        if loop.is_running():
            loop.close()


@pytest.fixture(scope="function")
async def kafka_producer() -> kafka.KafkaProducer:
    return mock.create_autospec(kafka.KafkaProducer)

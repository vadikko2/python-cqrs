import asyncio
from unittest import mock

import pytest

from cqrs.adapters import kafka

TEST_TOPIC = "TestCqrsTopic"

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for session-scoped fixtures."""
    loop = asyncio.new_event_loop()
    try:
        yield loop
    finally:
        if not loop.is_closed():
            loop.close()


@pytest.fixture(scope="function")
async def kafka_producer() -> kafka.KafkaProducer:
    producer = mock.create_autospec(kafka.KafkaProducer)
    producer.produce = mock.AsyncMock()
    return producer

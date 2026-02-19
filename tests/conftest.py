import pytest
from unittest import mock
from cqrs.adapters import kafka


TEST_TOPIC = "TestCqrsTopic"

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture(scope="function")
async def kafka_producer() -> kafka.KafkaProducer:
    producer = mock.create_autospec(kafka.KafkaProducer)
    producer.produce = mock.AsyncMock()
    return producer

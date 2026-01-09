import pytest
from unittest import mock
from cqrs.adapters import kafka


TEST_TOPIC = "TestCqrsTopic"

pytest_plugins = ["tests.integration.fixtures"]


@pytest.fixture(scope="function")
async def kafka_producer() -> kafka.KafkaProducer:
    return mock.create_autospec(kafka.KafkaProducer)

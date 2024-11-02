import asyncio
import functools
import logging


import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka
from cqrs.outbox import mock

logging.basicConfig(level=logging.DEBUG)

MOCK_STORAGE = dict()
repository = mock.MockOutboxedEventRepository(
    session_factory=functools.partial(lambda: MOCK_STORAGE),
)
repository.add(
    MOCK_STORAGE,
    cqrs.NotificationEvent(event_name="TestEvent", topic="TestTopic"),
)
repository.add(
    MOCK_STORAGE,
    cqrs.NotificationEvent(event_name="TestEvent", topic="TestTopic"),
)
repository.add(
    MOCK_STORAGE,
    cqrs.NotificationEvent(event_name="TestEvent", topic="TestTopic"),
)


async def main():
    broker = kafka.KafkaMessageBroker(
        producer=kafka_adapters.kafka_producer_factory(dsn="localhost:9092"),
    )
    producer = cqrs.EventProducer(message_broker=broker, repository=repository)
    await producer.periodically_task()


if __name__ == "__main__":
    print(
        "Run kafka infrastructure with: `docker compose -f ./docker-compose-examples.yml up -d`",
    )
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())

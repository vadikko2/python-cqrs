import asyncio
import functools
import logging
import typing
import uuid
from collections import defaultdict

import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka
from cqrs.outbox import mock

logging.basicConfig(level=logging.DEBUG)

MOCK_STORAGE = defaultdict[
    uuid.UUID,
    typing.List[cqrs.NotificationEvent],
](lambda: [])

cqrs.OutboxedEventMap.register(
    "empty_event",
    cqrs.NotificationEvent[typing.Dict],
)

repository = mock.MockOutboxedEventRepository(
    session_factory=functools.partial(lambda: MOCK_STORAGE),
)

repository.add(
    MOCK_STORAGE,
    cqrs.NotificationEvent[typing.Dict](
        event_name="empty_event",
        topic="empty_topic",
        payload={},
    ),
)
repository.add(
    MOCK_STORAGE,
    cqrs.NotificationEvent[typing.Dict](
        event_name="empty_event",
        topic="empty_topic",
        payload={},
    ),
)
repository.add(
    MOCK_STORAGE,
    cqrs.NotificationEvent[typing.Dict](
        event_name="empty_event",
        topic="empty_topic",
        payload={},
    ),
)


async def main():
    broker = kafka.KafkaMessageBroker(
        producer=kafka_adapters.kafka_producer_factory(dsn="localhost:9092"),
    )
    producer = cqrs.EventProducer(message_broker=broker, repository=repository)
    await producer.periodically_task()


if __name__ == "__main__":
    print(
        "Run kafka infrastructure with: `docker compose -f ./docker-compose-dev.yml up -d`",
    )
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())

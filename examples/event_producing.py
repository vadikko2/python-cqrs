import asyncio
import functools
import logging

import cqrs
from cqrs.message_brokers import devnull
from cqrs.outbox import mock

logging.basicConfig(level=logging.DEBUG)

broker = devnull.DevnullMessageBroker()
MOCK_STORAGE = dict()
repository = mock.MockOutboxedEventRepository(
    session_factory=functools.partial(lambda: MOCK_STORAGE),
)

producer = cqrs.EventProducer(message_broker=broker, repository=repository)
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

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(producer.periodically_task())

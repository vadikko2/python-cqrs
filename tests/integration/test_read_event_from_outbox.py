import functools
import logging
import typing
import uuid
from collections import defaultdict

import pydantic
import pytest

import cqrs
from cqrs.outbox import mock, sqlalchemy

logging.basicConfig(level=logging.DEBUG)


class RegisteredTestPayload(pydantic.BaseModel):
    a: int = 1
    b: int = 2


cqrs.OutboxedEventMap.register(
    "empty_event",
    cqrs.NotificationEvent[RegisteredTestPayload],
)


class NotRegisteredTestPayload(pydantic.BaseModel):
    a: int = 1
    b: int = 2


async def test_read_event_from_mock_outbox_positive():
    mock_storage = defaultdict[
        uuid.UUID,
        typing.List[cqrs.NotificationEvent],
    ](lambda: [])

    repository = mock.MockOutboxedEventRepository(
        session_factory=functools.partial(lambda: mock_storage),
    )
    repository.add(
        mock_storage,
        cqrs.NotificationEvent[RegisteredTestPayload](
            event_name="empty_event",
            topic="empty_topic",
            payload=RegisteredTestPayload(),
        ),
    )
    repository.add(
        mock_storage,
        cqrs.NotificationEvent[RegisteredTestPayload](
            event_name="empty_event",
            topic="empty_topic",
            payload=RegisteredTestPayload(),
        ),
    )
    repository.add(
        mock_storage,
        cqrs.NotificationEvent[RegisteredTestPayload](
            event_name="empty_event",
            topic="empty_topic",
            payload=RegisteredTestPayload(),
        ),
    )

    events = await repository.get_many(mock_storage)

    assert len(events) == 3
    assert isinstance(events[0].event.payload, RegisteredTestPayload)


async def test_read_event_from_sqlalchemy_outbox_positive(session):
    repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
        lambda: session,
    )
    repository.add(
        session,
        cqrs.NotificationEvent[RegisteredTestPayload](
            event_name="empty_event",
            topic="empty_topic",
            payload=RegisteredTestPayload(),
        ),
    )
    repository.add(
        session,
        cqrs.NotificationEvent[RegisteredTestPayload](
            event_name="empty_event",
            topic="empty_topic",
            payload=RegisteredTestPayload(),
        ),
    )
    repository.add(
        session,
        cqrs.NotificationEvent[RegisteredTestPayload](
            event_name="empty_event",
            topic="empty_topic",
            payload=RegisteredTestPayload(),
        ),
    )
    await repository.commit(session)

    events = await repository.get_many(session, 3)

    assert len(events) == 3
    assert isinstance(events[0].event.payload, RegisteredTestPayload)


async def test_add_unregistered_event_negative(session):
    repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
        lambda: session,
    )

    with pytest.raises(TypeError, match="Unknown event name for not_registered_event"):
        repository.add(
            session,
            cqrs.NotificationEvent[NotRegisteredTestPayload](
                event_name="not_registered_event",
                topic="empty_topic",
                payload=NotRegisteredTestPayload(),
            ),
        )


async def test_add_registered_event_name_negative(session):
    repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
        lambda: session,
    )

    with pytest.raises(TypeError):
        repository.add(
            session,
            cqrs.NotificationEvent[NotRegisteredTestPayload](
                event_name="empty_event",
                topic="empty_topic",
                payload=NotRegisteredTestPayload(),
            ),
        )

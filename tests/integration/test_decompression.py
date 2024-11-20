import typing

import cqrs
from cqrs.outbox import repository as outbox_repository


async def test_decompression_positive(session):
    """Проверяет что декомпрессия происходит корректно"""
    repository = cqrs.SqlAlchemyOutboxedEventRepository(
        lambda: session,
        cqrs.ZlibCompressor(),
    )
    event = cqrs.NotificationEvent[typing.Dict](
        event_name="TestEvent",
        payload={"foo": "bar"},
    )
    cqrs.OutboxedEventMap.register("TestEvent", cqrs.NotificationEvent[typing.Dict])

    repository.add(session, event)
    await repository.commit(session)

    read_event: outbox_repository.OutboxedEvent | None = next(
        iter(
            await repository.get_many(
                session,
                batch_size=1,
            ),
        ),
        None,
    )

    assert read_event
    assert read_event.event.event_id == event.event_id
    assert read_event.event.payload
    assert read_event.event.payload["foo"] == "bar"

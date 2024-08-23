import typing

import cqrs


async def test_decompression_positive(session):
    """Проверяет что декомпрессия происходит корректно"""
    repository = cqrs.SqlAlchemyOutboxedEventRepository(lambda: session, cqrs.ZlibCompressor())
    event = cqrs.ECSTEvent[typing.Dict](event_name="TestEvent", payload={"foo": "bar"})
    repository.add(session, event)
    await repository.commit(session)

    read_event = await repository.get_one(session, event.event_id)

    assert read_event
    assert read_event.event_id == event.event_id
    assert read_event.payload["foo"] == "bar"

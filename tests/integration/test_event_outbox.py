import typing

import pydantic

import cqrs
from cqrs import events, requests
from cqrs.outbox import (
    repository as outbox_repository,
    repository as repository_protocol,
    sqlalchemy,
)


class OutboxRequest(requests.Request):
    message: typing.Text
    count: int


class ECSTPayload(pydantic.BaseModel):
    message: typing.Text


cqrs.OutboxedEventMap.register(
    "OutboxRequestHandler",
    events.NotificationEvent[ECSTPayload],
)


class OutboxRequestHandler(requests.RequestHandler[OutboxRequest, None]):
    def __init__(self, repository: cqrs.OutboxedEventRepository):
        self.repository = repository

    @property
    def events(self) -> list[events.Event]:
        return []

    async def handle(self, request: OutboxRequest) -> None:
        async with self.repository as session:
            list(
                map(
                    lambda e: self.repository.add(session, e),
                    [
                        events.NotificationEvent[ECSTPayload](
                            event_name=OutboxRequestHandler.__name__,
                            payload=ECSTPayload(message=request.message),
                        )
                        for _ in range(request.count)
                    ],
                ),
            )
            await self.repository.commit(session)


class TestOutbox:
    async def test_outbox_add_3_event_positive(self, session):
        """
        checks positive save events to outbox case
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(message="test_outbox_add_3_event_positive", count=3)
        await OutboxRequestHandler(repository).handle(request)

        not_produced_events: typing.List[
            outbox_repository.OutboxedEvent
        ] = await repository.get_many(session, 3)
        await repository.commit(session)

        assert len(not_produced_events) == 3
        assert all(
            filter(
                lambda e: e.event.payload.message == "test_outbox_add_3_event_positive",
                not_produced_events,
            ),
        )
        assert all(
            filter(
                lambda e: e.event.event_name == "OutboxRequestHandler",  # type: ignore
                not_produced_events,
            ),
        )

    async def test_get_new_events_positive(self, session):
        """
        checks getting many new events
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=3,
        )
        await OutboxRequestHandler(repository).handle(request)

        events_list = await repository.get_many(session, 3)
        await repository.commit(session)

        assert len(events_list) == 3

    async def test_get_new_events_negative(self, session):
        """
        checks getting many new events, but not produced
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=3,
        )
        await OutboxRequestHandler(repository).handle(request)
        events_list = await repository.get_many(session, 3)
        await repository.update_status(
            session,
            events_list[-1].id,
            repository_protocol.EventStatus.PRODUCED,
        )
        await repository.commit(session)

        new_events_list = await repository.get_many(session, 3)
        await repository.commit(session)

        assert len(new_events_list) == 2

    async def test_get_new_event_positive(self, session):
        """
        checks getting one event positive
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(repository).handle(request)
        [event_over_get_all_events_method] = await repository.get_many(session, 1)

        event: outbox_repository.OutboxedEvent | None = next(
            iter(
                await repository.get_many(
                    session,
                    batch_size=1,
                ),
            ),
            None,
        )
        await repository.commit(session)

        assert event
        assert event.id == event_over_get_all_events_method.id  # noqa

    async def test_get_new_event_negative(self, session):
        """
        checks getting one event positive, but not produced
        """
        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(repository).handle(request)
        [event_over_get_all_events_method] = await repository.get_many(session, 1)
        await repository.update_status(
            session,
            event_over_get_all_events_method.id,
            repository_protocol.EventStatus.PRODUCED,
        )
        await repository.commit(session)

        event = await repository.get_many(
            session,
            batch_size=1,
        )
        await repository.commit(session)

        assert not event

    async def test_mark_as_failure_positive(self, session):
        """checks reading failure produced event successfully"""

        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=2,
        )
        await OutboxRequestHandler(repository).handle(request)
        [failure_event, success_event] = await repository.get_many(session, 2)
        # mark FIRST event as failure
        await repository.update_status(
            session,
            failure_event.id,
            repository_protocol.EventStatus.NOT_PRODUCED,
        )
        await repository.commit(session)

        produce_candidates = await repository.get_many(session, batch_size=2)

        assert len(produce_candidates) == 2
        # check events order by status
        assert produce_candidates[0].id == success_event.id
        assert produce_candidates[1].id == failure_event.id

    async def test_mark_as_failure_negative(self, session):
        """checks reading failure produced events with flush_counter speeding"""

        repository = sqlalchemy.SqlAlchemyOutboxedEventRepository(
            lambda: session,
            compressor=None,
        )
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(repository).handle(request)
        [failure_event] = await repository.get_many(session, 1)
        for _ in range(sqlalchemy.MAX_FLUSH_COUNTER_VALUE):
            await repository.update_status(
                session,
                failure_event.id,
                repository_protocol.EventStatus.NOT_PRODUCED,
            )

        await repository.commit(session)

        produce_candidates = await repository.get_many(session, batch_size=1)

        assert not len(produce_candidates)

import typing

import pydantic
import pytest
from sqlalchemy import orm as sqla_orm
import sqlalchemy as sqla

import cqrs
from cqrs import events, Request, RequestHandler
from cqrs.outbox import (
    repository as outbox_repository,
    repository as repository_protocol,
    sqlalchemy,
)


class Base(sqla_orm.DeclarativeBase):
    pass


class TestOutboxModel(Base, sqlalchemy.OutboxModelMixin):
    __tablename__ = "test_outbox"


class OutboxRequest(Request):
    message: typing.Text
    count: int


class ECSTPayload(pydantic.BaseModel):
    message: typing.Text


cqrs.OutboxedEventMap.register(
    "OutboxRequestHandler",
    events.NotificationEvent[ECSTPayload],
)


class OutboxRequestHandler(RequestHandler[OutboxRequest, None]):
    def __init__(self, repository: cqrs.OutboxedEventRepository):
        self.repository = repository

    @property
    def events(self) -> typing.Sequence[events.IEvent]:
        return []

    async def handle(self, request: OutboxRequest) -> None:
        list(
            map(
                lambda e: self.repository.add(e),
                [
                    events.NotificationEvent[ECSTPayload](
                        event_name=OutboxRequestHandler.__name__,
                        payload=ECSTPayload(message=request.message),
                    )
                    for _ in range(request.count)
                ],
            ),
        )
        await self.repository.commit()


@pytest.fixture
def outbox_repo(session):
    return sqlalchemy.SqlAlchemyOutboxedEventRepository(
        session,
        outbox_model=TestOutboxModel
    )


class TestOutbox:

    @pytest.fixture(autouse=True)
    async def setup_db(self, engine):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @pytest.fixture(autouse=True)
    async def cleanup_table(self, session, setup_db):
        await session.execute(sqla.delete(TestOutboxModel))
        await session.commit()
        yield

    async def test_outbox_add_3_event_positive(self, outbox_repo):
        """
        checks positive save events to outbox case
        """
        request = OutboxRequest(message="test_outbox_add_3_event_positive", count=3)
        await OutboxRequestHandler(outbox_repo).handle(request)

        not_produced_events: typing.List[
            outbox_repository.OutboxedEvent
        ] = await outbox_repo.get_many(3)
        await outbox_repo.commit()

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

    async def test_get_new_events_positive(self, outbox_repo):
        """
        checks getting many new events
        """
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=3,
        )
        await OutboxRequestHandler(outbox_repo).handle(request)

        events_list = await outbox_repo.get_many(3)
        await outbox_repo.commit()

        assert len(events_list) == 3

    async def test_get_new_events_negative(self, outbox_repo):
        """
        checks getting many new events, but not produced
        """
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=3,
        )
        await OutboxRequestHandler(outbox_repo).handle(request)
        events_list = await outbox_repo.get_many(3)
        await outbox_repo.update_status(
            events_list[-1].id,
            repository_protocol.EventStatus.PRODUCED,  # type: ignore[arg-type]
        )
        await outbox_repo.commit()

        new_events_list = await outbox_repo.get_many(3)
        await outbox_repo.commit()

        assert len(new_events_list) == 2

    async def test_get_new_event_positive(self, outbox_repo):
        """
        checks getting one event positive
        """
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(outbox_repo).handle(request)
        [event_over_get_all_events_method] = await outbox_repo.get_many(1)

        event: outbox_repository.OutboxedEvent | None = next(
            iter(
                await outbox_repo.get_many(
                    batch_size=1,
                ),
            ),
            None,
        )
        await outbox_repo.commit()

        assert event
        assert event.id == event_over_get_all_events_method.id

    async def test_get_new_event_negative(self, outbox_repo):
        """
        checks getting one event positive, but not produced
        """
        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(outbox_repo).handle(request)
        [event_over_get_all_events_method] = await outbox_repo.get_many(1)
        await outbox_repo.update_status(
            event_over_get_all_events_method.id,
            repository_protocol.EventStatus.PRODUCED,  # type: ignore[arg-type]
        )
        await outbox_repo.commit()

        event = await outbox_repo.get_many(
            batch_size=1,
        )
        await outbox_repo.commit()

        assert not event

    async def test_mark_as_failure_positive(self, outbox_repo):
        """checks reading failure produced event successfully"""

        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=2,
        )
        await OutboxRequestHandler(outbox_repo).handle(request)
        [failure_event, success_event] = await outbox_repo.get_many(2)
        # mark FIRST event as failure
        await outbox_repo.update_status(
            failure_event.id,
            repository_protocol.EventStatus.NOT_PRODUCED,  # type: ignore[arg-type]
        )
        await outbox_repo.commit()

        produce_candidates = await outbox_repo.get_many(batch_size=2)

        assert len(produce_candidates) == 2
        # check events order by status
        assert produce_candidates[0].id == success_event.id
        assert produce_candidates[1].id == failure_event.id

    async def test_mark_as_failure_negative(self, outbox_repo):
        """checks reading failure produced events with flush_counter speeding"""

        request = OutboxRequest(
            message="test_outbox_mark_event_as_produced_positive",
            count=1,
        )
        await OutboxRequestHandler(outbox_repo).handle(request)
        [failure_event] = await outbox_repo.get_many(1)
        for _ in range(sqlalchemy.MAX_FLUSH_COUNTER_VALUE):
            await outbox_repo.update_status(
                failure_event.id,
                repository_protocol.EventStatus.NOT_PRODUCED,  # type: ignore[arg-type]
            )

        await outbox_repo.commit()

        produce_candidates = await outbox_repo.get_many(batch_size=1)

        assert not len(produce_candidates)

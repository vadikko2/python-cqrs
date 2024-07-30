import typing

import cqrs
from cqrs import events, requests
from cqrs.outbox import sqlalchemy


class TestOutboxRequest(requests.Request):
    message: typing.Text
    count: int


class TestOutboxRequestHandler(requests.RequestHandler[TestOutboxRequest, None]):
    def __init__(self, outbox: cqrs.Outbox):
        self.outbox = outbox

    async def handle(self, request: TestOutboxRequest) -> None:
        list(
            map(
                self.outbox.add,
                [
                    events.ECSTEvent(
                        event_name=TestOutboxRequestHandler.__name__,
                        payload=dict(message=request.message),
                    )
                    for _ in range(request.count)
                ],
            ),
        )
        await self.outbox.save()


class TestOutbox:
    async def test_outbox_add_3_event_positive(self, session):
        """
        checks positive save events to outbox case
        """
        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_add_3_event_positive", count=3)
        await TestOutboxRequestHandler(outbox).handle(request)

        not_produced_events = await outbox.get_events(3)
        await session.commit()

        assert len(not_produced_events) == 3
        assert all(filter(lambda e: e.payload["message"] == "test_outbox_add_3_event_positive", not_produced_events))
        assert all(filter(lambda e: e.event_name == TestOutboxRequestHandler.__name__, not_produced_events))

    async def test_get_new_events_positive(self, session):
        """
        checks getting many new events
        """
        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_mark_event_as_produced_positive", count=3)
        await TestOutboxRequestHandler(outbox).handle(request)

        events_list = await outbox.get_events(3)
        await session.commit()

        assert len(events_list) == 3

    async def test_get_new_events_negative(self, session):
        """
        checks getting many new events, but not produced
        """
        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_mark_event_as_produced_positive", count=3)
        await TestOutboxRequestHandler(outbox).handle(request)
        events_list = await outbox.get_events(3)
        await outbox.mark_as_produced(events_list[-1].event_id)
        await outbox.save()

        new_events_list = await outbox.get_events(3)
        await session.commit()

        assert len(new_events_list) == 2

    async def test_get_new_event_positive(self, session):
        """
        checks getting one event positive
        """
        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_mark_event_as_produced_positive", count=1)
        await TestOutboxRequestHandler(outbox).handle(request)
        [event_over_get_all_events_method] = await outbox.get_events(1)

        event = await outbox.get_event(event_over_get_all_events_method.event_id)
        await session.commit()

        assert event.event_id == event_over_get_all_events_method.event_id

    async def test_get_new_event_negative(self, session):
        """
        checks getting one event positive, but not produced
        """
        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_mark_event_as_produced_positive", count=1)
        await TestOutboxRequestHandler(outbox).handle(request)
        [event_over_get_all_events_method] = await outbox.get_events(1)
        await outbox.mark_as_produced(event_over_get_all_events_method.event_id)
        await outbox.save()

        event = await outbox.get_event(event_over_get_all_events_method.event_id)
        await session.commit()

        assert event is None

    async def test_mark_as_failure_positive(self, session):
        """checks reading failure produced event successfully"""

        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_mark_event_as_produced_positive", count=2)
        await TestOutboxRequestHandler(outbox).handle(request)
        [failure_event, success_event] = await outbox.get_events(2)
        # mark FIRST event as failure
        await outbox.mark_as_failure(failure_event.event_id)
        await outbox.save()

        produce_candidates = await outbox.get_events(batch_size=2)

        assert len(produce_candidates) == 2
        # check events order by status
        assert produce_candidates[0].event_id == success_event.event_id
        assert produce_candidates[1].event_id == failure_event.event_id

    async def test_mark_as_failure_negative(self, session):
        """checks reading failure produced events with flush_counter speeding"""

        outbox = cqrs.SqlAlchemyOutbox(session)
        request = TestOutboxRequest(message="test_outbox_mark_event_as_produced_positive", count=1)
        await TestOutboxRequestHandler(outbox).handle(request)
        [failure_event] = await outbox.get_events(1)
        for _ in range(sqlalchemy.MAX_FLUSH_COUNTER_VALUE):
            await outbox.mark_as_failure(failure_event.event_id)

        await outbox.save()

        produce_candidates = await outbox.get_events(batch_size=1)

        assert not len(produce_candidates)

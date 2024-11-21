import typing

import cqrs
from cqrs.outbox import repository


class MockOutboxedEventRepository(repository.OutboxedEventRepository[typing.Dict]):
    COUNTER: typing.ClassVar = 0

    def __init__(self, session_factory: typing.Callable[[], typing.Dict]):
        self.session = session_factory()

    async def __aenter__(self) -> typing.Dict:
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def add(self, session: typing.Dict, event: cqrs.NotificationEvent) -> None:
        MockOutboxedEventRepository.COUNTER += 1
        session[MockOutboxedEventRepository.COUNTER] = repository.OutboxedEvent(
            id=MockOutboxedEventRepository.COUNTER,
            event=event,
            topic=event.topic,
            status=repository.EventStatus.NEW,
        )

    async def get_many(
        self,
        session: typing.Dict,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.OutboxedEvent]:
        return list(
            filter(lambda e: topic == e.topic, session.values())
            if topic
            else list(session.values()),
        )

    async def update_status(
        self,
        session: typing.Dict,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ):
        if outboxed_event_id not in session:
            return
        if new_status is repository.EventStatus.PRODUCED:
            del session[outboxed_event_id]

    async def commit(self, session: typing.Dict):
        pass

    async def rollback(self, session: typing.Dict):
        pass

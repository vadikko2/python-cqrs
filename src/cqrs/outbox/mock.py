import typing
import uuid

from cqrs.outbox import repository


class MockOutboxedEventRepository(repository.OutboxedEventRepository):
    async def __aenter__(self) -> typing.Dict:
        return self._session_factory()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def add(self, session: typing.Dict, event: repository.Event) -> None:
        session[event.event_id] = event

    async def get_one(
        self,
        session: typing.Dict,
        event_id: uuid.UUID,
    ) -> repository.Event | None:
        return session.get(event_id)

    async def get_many(
        self,
        session: typing.Dict,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.Event]:
        return list(
            filter(lambda e: e.topic == topic, session.values())
            if topic
            else list(session.values()),
        )

    async def update_status(
        self,
        session: typing.Dict,
        event_id: uuid.UUID,
        new_status: repository.EventStatus,
    ):
        if event_id not in session:
            return
        if new_status is repository.EventStatus.PRODUCED:
            del session[event_id]

    async def commit(self, session: typing.Dict):
        pass

    async def rollback(self, session: typing.Dict):
        pass

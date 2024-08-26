import abc
import asyncio
import typing
import uuid

from cqrs.events import event as ev

Event: typing.TypeAlias = ev.NotificationEvent | ev.ECSTEvent


class Outbox(typing.Protocol):
    def add(self, event: Event):
        """Adds event to outbox"""
        raise NotImplementedError

    async def save(self):
        """Commits events to the storage"""
        raise NotImplementedError

    async def get_events(self, batch_size: int = 100) -> typing.List[Event]:
        """Returns not produced events"""
        raise NotImplementedError

    async def get_event(self, event_id: uuid.UUID) -> Event | None:
        """Returns event by id"""
        raise NotImplementedError

    async def mark_as_produced(self, event_id: uuid.UUID) -> None:
        """Marks event as produced"""
        raise NotImplementedError

    async def mark_as_failure(self, event_id: uuid.UUID) -> None:
        """Marks event as not produced with failure"""
        raise NotImplementedError


class EventProducer(abc.ABC):
    @abc.abstractmethod
    async def produce_one(self, event_id: uuid.UUID) -> None:
        """Produces event to broker"""
        raise NotImplementedError

    @abc.abstractmethod
    async def produce_batch(self, batch_size: int = 100) -> None:
        """Produces events to broker"""
        raise NotImplementedError

    async def periodically_task(
        self,
        batch_size: int = 100,
        wait_ms: int = 500,
    ) -> None:
        """Calls produce periodically with specified delay"""
        while True:
            await asyncio.sleep(float(wait_ms) / 1000.0)
            await self.produce_batch(batch_size)

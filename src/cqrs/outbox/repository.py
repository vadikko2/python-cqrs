import abc
import enum
import typing

import pydantic

import cqrs
from cqrs.events.event import NotificationEvent


class EventStatus(enum.StrEnum):
    NEW = "new"
    PRODUCED = "produced"
    NOT_PRODUCED = "not_produced"


class OutboxedEvent(pydantic.BaseModel, frozen=True):
    id: pydantic.PositiveInt
    event: cqrs.NotificationEvent
    topic: typing.Text
    status: EventStatus


class OutboxedEventRepository(abc.ABC):
    @abc.abstractmethod
    def add(
        self,
        event: NotificationEvent,
    ) -> None:
        """Add an event to the repository."""

    @abc.abstractmethod
    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[OutboxedEvent]:
        """Get many events from the repository."""

    @abc.abstractmethod
    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: EventStatus,
    ):
        """Update the event status"""

    @abc.abstractmethod
    async def commit(self):
        pass

    @abc.abstractmethod
    async def rollback(self):
        pass

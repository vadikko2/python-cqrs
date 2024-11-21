import abc
import enum
import typing

import pydantic

import cqrs
from cqrs.events import event as ev

Session = typing.TypeVar("Session")


class EventStatus(enum.StrEnum):
    NEW = "new"
    PRODUCED = "produced"
    NOT_PRODUCED = "not_produced"


class OutboxedEvent(pydantic.BaseModel, frozen=True):
    id: pydantic.PositiveInt
    event: cqrs.NotificationEvent
    topic: typing.Text
    status: EventStatus


class OutboxedEventRepository(abc.ABC, typing.Generic[Session]):
    @abc.abstractmethod
    async def __aenter__(self) -> Session:
        """start transaction"""

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """end transaction"""

    @abc.abstractmethod
    def add(
        self,
        session: Session,
        event: ev.NotificationEvent,
    ) -> None:
        """Add an event to the repository."""

    @abc.abstractmethod
    async def get_many(
        self,
        session: Session,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[OutboxedEvent]:
        """Get many events from the repository."""

    @abc.abstractmethod
    async def update_status(
        self,
        session: Session,
        outboxed_event_id: int,
        new_status: EventStatus,
    ):
        """Update the event status"""

    @abc.abstractmethod
    async def commit(self, session: Session):
        """Commit the changes to the repository."""

    @abc.abstractmethod
    async def rollback(self, session: Session):
        """Rollback the changes to the repository."""

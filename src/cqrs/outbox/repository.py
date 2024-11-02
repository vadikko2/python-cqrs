import abc
import enum
import typing
import uuid

import cqrs
from cqrs.events import event as ev

Event = typing.TypeVar("Event", ev.NotificationEvent, ev.ECSTEvent, contravariant=True)
Session = typing.TypeVar("Session")


class EventStatus(enum.StrEnum):
    NEW = "new"
    PRODUCED = "produced"
    NOT_PRODUCED = "not_produced"


class OutboxedEventRepository(abc.ABC, typing.Generic[Session]):
    def __init__(
        self,
        session_factory: typing.Callable[[], Session],
        compressor: cqrs.Compressor | None = None,
    ):
        self._session_factory = session_factory
        self._compressor = compressor

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
        event: Event,
    ) -> None:
        """Add an event to the repository."""

    @abc.abstractmethod
    async def get_one(self, session: Session, event_id: uuid.UUID) -> Event | None:
        """Get one event from the repository."""

    @abc.abstractmethod
    async def get_many(
        self,
        session: Session,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[Event]:
        """Get many events from the repository."""

    @abc.abstractmethod
    async def update_status(
        self,
        session: Session,
        event_id: uuid.UUID,
        new_status: EventStatus,
    ):
        """Update the event status"""

    @abc.abstractmethod
    async def commit(self, session: Session):
        """Commit the changes to the repository."""

    @abc.abstractmethod
    async def rollback(self, session: Session):
        """Rollback the changes to the repository."""

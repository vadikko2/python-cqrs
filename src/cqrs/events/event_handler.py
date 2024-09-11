import abc
import typing

from cqrs.events import event as event_models

E = typing.TypeVar("E", bound=event_models.Event, contravariant=True)


class EventHandler(abc.ABC, typing.Generic[E]):
    """
    The event handler interface.

    Usage::

      class UserJoinedEventHandler(EventHandler[UserJoinedEventHandler])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api

          async def handle(self, event: UserJoinedEventHandler) -> None:
              await self._meetings_api.notify_room(event.meeting_id, "New user joined!")

    """

    @abc.abstractmethod
    async def handle(self, event: E) -> None:
        raise NotImplementedError


class SyncEventHandler(abc.ABC, typing.Generic[E]):
    """
    The event handler interface.

    Usage::

      class UserJoinedEventHandler(SyncEventHandler[UserJoinedEventHandler])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api

          def handle(self, event: UserJoinedEventHandler) -> None:
              self._meetings_api.notify_room(event.meeting_id, "New user joined!")

    """

    @abc.abstractmethod
    def handle(self, event: E) -> None:
        raise NotImplementedError

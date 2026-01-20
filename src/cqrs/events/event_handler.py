import abc
import typing

from cqrs.events.event import IEvent

E = typing.TypeVar("E", bound=IEvent, contravariant=True)


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

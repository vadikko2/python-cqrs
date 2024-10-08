import abc
import typing

from cqrs import response
from cqrs.events import event
from cqrs.requests import request as r

_Req = typing.TypeVar("_Req", bound=r.Request, contravariant=True)
_Resp = typing.TypeVar("_Resp", response.Response, None, covariant=True)


class RequestHandler(abc.ABC, typing.Generic[_Req, _Resp]):
    """
    The request handler interface.

    The request handler is an object, which gets a request as input and may return a response as a result.

    Command handler example::

      class JoinMeetingCommandHandler(RequestHandler[JoinMeetingCommand, None])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api
              self.events: list[Event] = []

          async def handle(self, request: JoinMeetingCommand) -> None:
              await self._meetings_api.join_user(request.user_id, request.meeting_id)

    Query handler example::

      class ReadMeetingQueryHandler(RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api
              self.events: list[Event] = []

          async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
              link = await self._meetings_api.get_link(request.meeting_id)
              return ReadMeetingQueryResult(link=link, meeting_id=request.meeting_id)

    """

    @property
    @abc.abstractmethod
    def events(self) -> typing.List[event.Event]:
        raise NotImplementedError

    @abc.abstractmethod
    async def handle(self, request: _Req) -> _Resp:
        raise NotImplementedError


class SyncRequestHandler(abc.ABC, typing.Generic[_Req, _Resp]):
    """
    The synchronous request handler interface.

    The request handler is an object, which gets a request as input and may return a response as a result.

    Command handler example::

      class JoinMeetingCommandHandler(SyncRequestHandler[JoinMeetingCommand, None])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api
              self.events: list[Event] = []

          def handle(self, request: JoinMeetingCommand) -> None:
              self._meetings_api.join_user(request.user_id, request.meeting_id)

    Query handler example::

      class ReadMeetingQueryHandler(SyncRequestHandler[ReadMeetingQuery, ReadMeetingQueryResult])
          def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
              self._meetings_api = meetings_api
              self.events: list[Event] = []

          def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
              link = self._meetings_api.get_link(request.meeting_id)
              return ReadMeetingQueryResult(link=link, meeting_id=request.meeting_id)
    """

    @property
    @abc.abstractmethod
    def events(self) -> typing.List[event.Event]:
        raise NotImplementedError

    @abc.abstractmethod
    def handle(self, request: _Req) -> _Resp:
        raise NotImplementedError

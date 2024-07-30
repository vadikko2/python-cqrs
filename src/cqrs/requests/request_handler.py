import typing

from cqrs import response
from cqrs.events import event
from cqrs.requests import request

Req = typing.TypeVar("Req", bound=request.Request, contravariant=True)
Res = typing.TypeVar("Res", response.Response, None, covariant=True)


class RequestHandler(typing.Protocol[Req, Res]):
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
    def events(self) -> list[event.Event]:
        ...

    async def handle(self, request: Req) -> Res:
        raise NotImplementedError

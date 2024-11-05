import asyncio
import logging

import di
import pydantic

import cqrs
from cqrs.message_brokers import devnull
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.DEBUG)


class UserJoinedNotificationPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedECSTPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        self._events.append(
            cqrs.NotificationEvent[UserJoinedNotificationPayload](
                event_name="UserJoined",
                topic="user_notification_events",
                payload=UserJoinedNotificationPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )
        self._events.append(
            cqrs.ECSTEvent[UserJoinedECSTPayload](
                event_name="UserJoined",
                topic="user_ecst_events",
                payload=UserJoinedECSTPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            ),
        )


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


async def main():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        message_broker=devnull.DevnullMessageBroker(),
    )
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="2"))
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="3"))

    assert len(devnull.MESSAGE_BUS) == 6  # type: ignore


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())

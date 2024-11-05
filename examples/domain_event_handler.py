import asyncio
import logging
import typing
from collections import defaultdict

import di

import cqrs
from cqrs.requests import bootstrap

logging.basicConfig(level=logging.DEBUG)
STORAGE = defaultdict[str, typing.List[str]](lambda: [])
HANDLED_EVENTS = set[cqrs.DomainEvent]()


class JoinMeetingCommand(cqrs.Request):
    user_id: str
    meeting_id: str


class UserJoined(cqrs.DomainEvent, frozen=True):
    user_id: str
    meeting_id: str


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        STORAGE[request.meeting_id].append(request.user_id)
        self._events.append(
            UserJoined(user_id=request.user_id, meeting_id=request.meeting_id),
        )
        print(f"User {request.user_id} joined meeting {request.meeting_id}")


class UserJoinedEventHandler(cqrs.EventHandler[UserJoined]):
    async def handle(self, event: UserJoined) -> None:
        print(f"Handle user {event.user_id} joined meeting {event.meeting_id} event")
        HANDLED_EVENTS.add(event)


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def events_mapper(mapper: cqrs.EventMap) -> None:
    mapper.bind(UserJoined, UserJoinedEventHandler)


async def main():
    mediator = bootstrap.bootstrap(
        di_container=di.Container(),
        commands_mapper=command_mapper,
        domain_events_mapper=events_mapper,
    )
    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="4", meeting_id="1"))
    print("There are {} users in the room".format(len(STORAGE["1"])))
    print("{} events was handled".format(len(HANDLED_EVENTS)))
    assert len(HANDLED_EVENTS) == 4


if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import functools
import typing
import uuid
from collections import defaultdict

import di
import pydantic
from di import dependent

import cqrs
from cqrs.outbox import mock
from cqrs.requests import bootstrap

OUTBOX_STORAGE = defaultdict[
    uuid.UUID,
    typing.List[cqrs.NotificationEvent | cqrs.ECSTEvent],
](lambda: [])
mock_repository_factory = functools.partial(
    mock.MockOutboxedEventRepository,
    session_factory=functools.partial(lambda: OUTBOX_STORAGE),
)


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
    def __init__(self, outbox: cqrs.OutboxedEventRepository):
        self.outbox = outbox

    @property
    def events(self):
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        async with self.outbox as session:
            self.outbox.add(
                session,
                cqrs.NotificationEvent[UserJoinedNotificationPayload](
                    event_name="UserJoined",
                    topic="user_notification_events",
                    payload=UserJoinedNotificationPayload(
                        user_id=request.user_id,
                        meeting_id=request.meeting_id,
                    ),
                ),
            )
            self.outbox.add(
                session,
                cqrs.ECSTEvent[UserJoinedECSTPayload](
                    event_name="UserJoined",
                    topic="user_ecst_events",
                    payload=UserJoinedECSTPayload(
                        user_id=request.user_id,
                        meeting_id=request.meeting_id,
                    ),
                ),
            )
            await self.outbox.commit(session)


def command_mapper(mapper: cqrs.RequestMap) -> None:
    mapper.bind(JoinMeetingCommand, JoinMeetingCommandHandler)


def setup_di() -> di.Container:
    """
    Initialize DI container
    """
    container = di.Container()
    bind = di.bind_by_type(
        dependent.Dependent(mock_repository_factory, scope="request"),
        cqrs.OutboxedEventRepository,
    )
    container.bind(bind)
    return container


async def main():
    mediator = bootstrap.bootstrap(
        di_container=setup_di(),
        commands_mapper=command_mapper,
    )
    repository = mock_repository_factory()

    await mediator.send(JoinMeetingCommand(user_id="1", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="2", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="3", meeting_id="1"))
    await mediator.send(JoinMeetingCommand(user_id="4", meeting_id="1"))

    notification_events = await repository.get_many(
        session=OUTBOX_STORAGE,
        topic="user_notification_events",
    )
    ecst_events = await repository.get_many(
        session=OUTBOX_STORAGE,
        topic="user_ecst_events",
    )

    assert len(OUTBOX_STORAGE) == 8
    assert len(notification_events) == 4
    assert len(ecst_events) == 4

    print("There are {} users in the room".format(len(OUTBOX_STORAGE)))
    print(f"There are {len(notification_events)} notification events in the outbox")
    print(f"There are {len(ecst_events)} ecst events in the outbox")
    print(f"Outbox storage: {OUTBOX_STORAGE}")


if __name__ == "__main__":
    asyncio.run(main())

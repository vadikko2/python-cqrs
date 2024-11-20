import typing
import uuid

import pydantic
import pytest

import cqrs
from cqrs import events, requests
from cqrs.message_brokers import kafka as kafka_broker


class CloseMeetingRoomCommand(requests.Request):
    meeting_room_id: uuid.UUID = pydantic.Field()


class CloseMeetingRoomCommandHandler(
    requests.RequestHandler[CloseMeetingRoomCommand, None],
):
    def __init__(self) -> None:
        self.called = False
        self._events: typing.List[events.Event] = []

    @property
    def events(self) -> typing.List:
        return self._events

    async def handle(self, request: CloseMeetingRoomCommand) -> None:
        self.called = True
        event = events.NotificationEvent(
            event_name="",
            payload=dict(
                meeting_room_id=request.meeting_room_id,
            ),
        )
        self._events.append(event)


class MockContainer:
    command_handler = CloseMeetingRoomCommandHandler()

    async def resolve(self, type_: typing.Type):
        if isinstance(self.command_handler, type_):
            return self.command_handler


@pytest.fixture
async def mediator(kafka_producer) -> cqrs.RequestMediator:
    broker = kafka_broker.KafkaMessageBroker(kafka_producer)
    event_emitter = events.EventEmitter(
        event_map=events.EventMap(),
        container=MockContainer(),  # type: ignore
        message_broker=broker,
    )
    request_map = requests.RequestMap()
    request_map.bind(CloseMeetingRoomCommand, CloseMeetingRoomCommandHandler)
    return cqrs.RequestMediator(
        request_map=request_map,
        container=MockContainer(),  # type: ignore
        event_emitter=event_emitter,
    )


async def test_produce_some_event(
    mediator: cqrs.RequestMediator,
    kafka_producer,
) -> None:
    """Тестируется вызов метода KafkaProducer.produce при наличии нотификационных событий"""

    handler: CloseMeetingRoomCommandHandler | None = await MockContainer().resolve(
        CloseMeetingRoomCommandHandler,
    )  # noqa
    command = CloseMeetingRoomCommand(meeting_room_id=uuid.uuid4())

    await mediator.send(command)

    assert handler
    assert handler.called
    assert kafka_producer.produce.called

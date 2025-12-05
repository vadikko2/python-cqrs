import typing
from unittest import mock

import pydantic

from cqrs import events
from cqrs.events import (
    DomainEvent,
    Event,
    EventEmitter,
    EventHandler,
    EventMap,
)
from cqrs.mediator import RequestMediator
from cqrs.requests import (
    Request,
    RequestHandler,
    RequestMap,
)
from cqrs.response import Response


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemsCommandHandler(RequestHandler[ProcessItemsCommand, None]):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(self, request: ProcessItemsCommand) -> None:
        self.called = True
        for item_id in request.item_ids:
            event = ItemProcessedDomainEvent(item_id=item_id)
            self._events.append(event)


class ItemProcessedDomainEvent(DomainEvent):  # type: ignore[misc]
    item_id: str = pydantic.Field()


class ItemProcessedEventHandler(EventHandler[ItemProcessedDomainEvent]):
    def __init__(self) -> None:
        self.processed_events: list[ItemProcessedDomainEvent] = []

    async def handle(self, event: ItemProcessedDomainEvent) -> None:
        self.processed_events.append(event)


class Container:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_request_mediator_processes_events_parallel() -> None:
    handler = ProcessItemsCommandHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
    container = Container(handler)

    event_handler = ItemProcessedEventHandler()
    event_map = EventMap()
    event_map.bind(ItemProcessedDomainEvent, ItemProcessedEventHandler)

    class EventContainer:
        def __init__(self, handler):
            self._handler = handler

        async def resolve(self, type_):
            return self._handler

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock.AsyncMock()

    mediator = RequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    mediator._event_dispatcher._container = EventContainer(event_handler)  # type: ignore

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    await mediator.send(request)

    assert handler.called
    assert len(event_handler.processed_events) == 3
    assert event_emitter.emit.call_count == 3


async def test_request_mediator_processes_events_sequentially() -> None:
    handler = ProcessItemsCommandHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
    container = Container(handler)

    event_handler = ItemProcessedEventHandler()
    event_map = EventMap()
    event_map.bind(ItemProcessedDomainEvent, ItemProcessedEventHandler)

    class EventContainer:
        def __init__(self, handler):
            self._handler = handler

        async def resolve(self, type_):
            return self._handler

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock.AsyncMock()

    mediator = RequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    mediator._event_dispatcher._container = EventContainer(event_handler)  # type: ignore

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    await mediator.send(request)

    assert handler.called
    assert len(event_handler.processed_events) == 3
    assert event_emitter.emit.call_count == 3

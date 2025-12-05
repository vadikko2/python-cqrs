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
    NotificationEvent,
)
from cqrs.mediator import StreamingRequestMediator
from cqrs.requests import (
    Request,
    RequestMap,
    StreamingRequestHandler,
)
from cqrs.response import Response


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()


class StreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(  # type: ignore
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        self.called = True
        for item_id in request.item_ids:
            result = ProcessItemResult(item_id=item_id, status="processed")
            event = NotificationEvent(
                event_name="ItemProcessed",
                payload={"item_id": item_id},
            )
            self._events.append(event)
            yield result


class Container:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_streaming_mediator_logic() -> None:
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock.AsyncMock()

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert handler.called
    assert len(results) == 3
    assert results[0].item_id == "item1"
    assert results[1].item_id == "item2"
    assert results[2].item_id == "item3"

    assert event_emitter.emit.call_count == 3


async def test_streaming_mediator_without_event_emitter() -> None:
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=None,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert handler.called
    assert len(results) == 2


async def test_streaming_mediator_events_order() -> None:
    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    emitted_events = []

    async def mock_emit(event: Event):
        emitted_events.append(event)

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock_emit

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)
        assert len(emitted_events) == len(results)

    assert len(emitted_events) == 2
    assert isinstance(emitted_events[0], events.NotificationEvent)
    assert isinstance(emitted_events[1], events.NotificationEvent)
    assert emitted_events[0].payload["item_id"] == "item1"  # type: ignore
    assert emitted_events[1].payload["item_id"] == "item2"  # type: ignore


class ItemProcessedDomainEvent(DomainEvent):  # type: ignore[misc]
    item_id: str = pydantic.Field()


class ItemProcessedEventHandler(EventHandler[ItemProcessedDomainEvent]):
    def __init__(self) -> None:
        self.processed_events: list[ItemProcessedDomainEvent] = []

    async def handle(self, event: ItemProcessedDomainEvent) -> None:
        self.processed_events.append(event)


class EventHandlerStreamingHandler(
    StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(  # type: ignore
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        self.called = True
        for item_id in request.item_ids:
            result = ProcessItemResult(item_id=item_id, status="processed")
            event = ItemProcessedDomainEvent(item_id=item_id)
            self._events.append(event)
            yield result


async def test_streaming_mediator_processes_events_parallel() -> None:
    handler = EventHandlerStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EventHandlerStreamingHandler)
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

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
    )

    mediator._event_dispatcher._container = EventContainer(event_handler)  # type: ignore

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert handler.called
    assert len(results) == 3
    assert len(event_handler.processed_events) == 3
    assert event_emitter.emit.call_count == 3


async def test_streaming_mediator_processes_events_sequentially() -> None:
    handler = EventHandlerStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EventHandlerStreamingHandler)
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

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    mediator._event_dispatcher._container = EventContainer(event_handler)  # type: ignore

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert handler.called
    assert len(results) == 3
    assert len(event_handler.processed_events) == 3
    assert event_emitter.emit.call_count == 3

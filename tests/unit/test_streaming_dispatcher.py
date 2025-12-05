import typing
from uuid import UUID, uuid4

import pydantic
import pytest

from cqrs.dispatcher import StreamingRequestDispatcher
from cqrs.events import DomainEvent, Event, EventHandler, EventMap, NotificationEvent
from cqrs.requests import (
    Request,
    RequestMap,
    StreamingRequestHandler,
    SyncStreamingRequestHandler,
)
from cqrs.response import Response


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()


class AsyncStreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
        self, request: ProcessItemsCommand
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


class SyncStreamingHandler(
    SyncStreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]
):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    def handle(self, request: ProcessItemsCommand) -> typing.Iterator[ProcessItemResult]:
        self.called = True
        for item_id in request.item_ids:
            result = ProcessItemResult(item_id=item_id, status="processed")
            event = NotificationEvent(
                event_name="ItemProcessed",
                payload={"item_id": item_id},
            )
            self._events.append(event)
            yield result


class StreamingContainer:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_async_streaming_dispatcher_logic() -> None:
    handler = AsyncStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, AsyncStreamingHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in dispatcher.dispatch(request):
        results.append(result)

    assert handler.called
    assert len(results) == 3
    assert results[0].response.item_id == "item1"
    assert results[1].response.item_id == "item2"
    assert results[2].response.item_id == "item3"
    assert len(results[0].events) == 1
    assert len(results[1].events) == 1
    assert len(results[2].events) == 1
    assert results[0].events[0].payload["item_id"] == "item1"  # type: ignore
    assert results[1].events[0].payload["item_id"] == "item2"  # type: ignore
    assert results[2].events[0].payload["item_id"] == "item3"  # type: ignore


async def test_sync_streaming_dispatcher_logic() -> None:
    handler = SyncStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, SyncStreamingHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in dispatcher.dispatch(request):
        results.append(result)

    assert handler.called
    assert len(results) == 2
    assert results[0].response.item_id == "item1"
    assert results[1].response.item_id == "item2"
    assert len(results[0].events) == 1
    assert len(results[1].events) == 1
    assert results[0].events[0].payload["item_id"] == "item1"  # type: ignore
    assert results[1].events[0].payload["item_id"] == "item2"  # type: ignore


async def test_streaming_dispatcher_empty_generator() -> None:
    handler = AsyncStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, AsyncStreamingHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=[])
    results = []
    async for result in dispatcher.dispatch(request):
        results.append(result)

    assert handler.called
    assert len(results) == 0


async def test_streaming_dispatcher_handler_not_found() -> None:
    request_map = RequestMap()
    container = StreamingContainer(None)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1"])

    with pytest.raises(Exception) as exc_info:
        async for _ in dispatcher.dispatch(request):
            pass

    assert "StreamingRequestHandler not found" in str(exc_info.value)




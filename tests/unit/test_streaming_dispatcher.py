import typing
from unittest import mock

import pydantic
import pytest

import cqrs
from cqrs.dispatcher import StreamingRequestDispatcher
from cqrs.events import Event, NotificationEvent
from cqrs.requests.map import RequestMap
from cqrs.requests.request import Request
from cqrs.requests.request_handler import StreamingRequestHandler
from cqrs.response import Response


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()


class AsyncStreamingHandler(
    StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
):
    def __init__(self) -> None:
        self.called = False
        self._events: list[Event] = []

    @property
    def events(self) -> typing.Sequence[cqrs.IEvent]:
        return self._events.copy()

    def clear_events(self) -> None:
        self._events.clear()

    async def handle(
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


class StreamingContainer:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_streaming_handler_handle_returns_async_iterator_consumable_with_async_for() -> None:
    """
    Contract: StreamingRequestHandler.handle(request) is called without await
    and returns an AsyncIterator that is consumed with async for.
    """
    handler = AsyncStreamingHandler()
    request = ProcessItemsCommand(item_ids=["a", "b"])
    # handle() is called (no await) and returns async generator
    async_gen = handler.handle(request)
    # Can be iterated with async for
    results = []
    async for item in async_gen:
        results.append(item)
    assert len(results) == 2
    assert results[0].item_id == "a"
    assert results[1].item_id == "b"
    assert handler.called


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


async def test_async_streaming_dispatcher_calls_clear_events() -> None:
    handler = AsyncStreamingHandler()
    handler.clear_events = mock.Mock(wraps=handler.clear_events)  # type: ignore

    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, AsyncStreamingHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    async for _ in dispatcher.dispatch(request):
        pass

    assert handler.clear_events.call_count == 3


async def test_streaming_dispatcher_rejects_non_async_generator_handler() -> None:
    """Test that dispatcher raises TypeError if handler.handle is not an async generator."""

    class NonAsyncGeneratorHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[cqrs.IEvent] = []

        @property
        def events(self) -> typing.Sequence[cqrs.IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        # This is not an async generator (missing yield)
        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            return typing.cast(typing.AsyncIterator[ProcessItemResult], None)  # type: ignore

    handler = NonAsyncGeneratorHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, NonAsyncGeneratorHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1"])

    with pytest.raises(TypeError, match="must be an async generator function"):
        async for _ in dispatcher.dispatch(request):
            pass


# Test removed: RequestMap.bind_cor doesn't exist
# The COR pattern is not applicable to streaming dispatchers


async def test_streaming_dispatcher_with_middleware_chain() -> None:
    """Test that dispatcher works with middleware chain."""
    from cqrs.middlewares.base import Middleware, MiddlewareChain

    middleware_called = []

    class TestMiddleware(Middleware):
        async def __call__(self, request, call_next):
            middleware_called.append("before")
            result = await call_next(request)
            middleware_called.append("after")
            return result

    handler = AsyncStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, AsyncStreamingHandler)
    container = StreamingContainer(handler)

    middleware_chain = MiddlewareChain()
    middleware_chain.add(TestMiddleware())

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
        middleware_chain=middleware_chain,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in dispatcher.dispatch(request):
        results.append(result)

    assert len(results) == 2
    assert handler.called


async def test_streaming_dispatcher_preserves_event_order() -> None:
    """Test that events are yielded in the same order as items are processed."""
    handler = AsyncStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, AsyncStreamingHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    item_ids = ["first", "second", "third"]
    request = ProcessItemsCommand(item_ids=item_ids)
    results = []
    async for result in dispatcher.dispatch(request):
        results.append(result)

    assert len(results) == 3
    for i, result in enumerate(results):
        assert result.response.item_id == item_ids[i]
        assert len(result.events) == 1
        assert result.events[0].payload["item_id"] == item_ids[i]  # type: ignore


async def test_streaming_dispatcher_handler_exception_propagates() -> None:
    """Test that exceptions in handler are propagated to caller."""

    class FailingHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[cqrs.IEvent] = []

        @property
        def events(self) -> typing.Sequence[cqrs.IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for i, item_id in enumerate(request.item_ids):
                if i == 1:
                    raise ValueError(f"Failed processing {item_id}")
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = FailingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, FailingHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []

    with pytest.raises(ValueError, match="Failed processing item2"):
        async for result in dispatcher.dispatch(request):
            results.append(result)

    # Should have processed first item before exception
    assert len(results) == 1
    assert results[0].response.item_id == "item1"


async def test_streaming_dispatcher_multiple_events_per_yield() -> None:
    """Test handler that emits multiple events per yield."""

    class MultiEventHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[cqrs.IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for item_id in request.item_ids:
                # Emit multiple events per item
                self._events.append(
                    NotificationEvent(
                        event_name="ItemStarted",
                        payload={"item_id": item_id},
                    )
                )
                self._events.append(
                    NotificationEvent(
                        event_name="ItemProcessed",
                        payload={"item_id": item_id},
                    )
                )
                self._events.append(
                    NotificationEvent(
                        event_name="ItemCompleted",
                        payload={"item_id": item_id},
                    )
                )
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = MultiEventHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, MultiEventHandler)
    container = StreamingContainer(handler)

    dispatcher = StreamingRequestDispatcher(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in dispatcher.dispatch(request):
        results.append(result)

    assert len(results) == 2
    # Each yield should have 3 events
    assert len(results[0].events) == 3
    assert len(results[1].events) == 3
    # Events should be cleared after each yield
    assert len(handler.events) == 0
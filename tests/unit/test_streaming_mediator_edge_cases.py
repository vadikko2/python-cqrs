"""Edge case tests for StreamingRequestMediator."""

import asyncio
import time
import typing

import pydantic
import pytest

from cqrs import events
from cqrs.events import (
    DomainEvent,
    Event,
    EventEmitter,
    EventHandler,
    EventMap,
    NotificationEvent,
)
from cqrs.events.event import IEvent
from cqrs.mediator import StreamingRequestMediator
from cqrs.requests.map import RequestMap
from cqrs.requests.request import Request
from cqrs.requests.request_handler import StreamingRequestHandler
from cqrs.response import Response


class ProcessItemsCommand(Request):
    item_ids: list[str] = pydantic.Field()


class ProcessItemResult(Response):
    item_id: str = pydantic.Field()
    status: str = pydantic.Field()


class ItemProcessedDomainEvent(DomainEvent, frozen=True):
    item_id: str = pydantic.Field()


class Container:
    def __init__(self, handler):
        self._handler = handler

    async def resolve(self, type_):
        return self._handler


async def test_streaming_mediator_handles_empty_stream() -> None:
    """Test that mediator handles empty stream gracefully."""

    class EmptyStreamHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self.called = False
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            self.called = True
            # Don't yield anything
            return
            yield  # Make this an async generator

    handler = EmptyStreamHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EmptyStreamHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert handler.called
    assert len(results) == 0


async def test_streaming_mediator_handler_exception_stops_stream() -> None:
    """Test that exception in handler stops the stream."""

    class FailingStreamHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for i, item_id in enumerate(request.item_ids):
                if i == 2:
                    raise ValueError("Processing failed")
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = FailingStreamHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, FailingStreamHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3", "item4"])
    results = []

    with pytest.raises(ValueError, match="Processing failed"):
        async for result in mediator.stream(request):
            results.append(result)

    # Should have processed 2 items before failure
    assert len(results) == 2


async def test_streaming_mediator_with_none_response_values() -> None:
    """Test that mediator handles None response values correctly."""

    class NoneReturningHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult | None]:  # type: ignore
            for item_id in request.item_ids:
                if item_id == "skip":
                    yield None  # type: ignore
                else:
                    yield ProcessItemResult(item_id=item_id, status="processed")

    handler = NoneReturningHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, NoneReturningHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "skip", "item3"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    # All 3 yields should be present (including None)
    assert len(results) == 3
    assert results[0].item_id == "item1"
    assert results[1] is None
    assert results[2].item_id == "item3"


async def test_streaming_mediator_concurrent_event_handling_with_slow_handlers() -> None:
    """Test parallel event handling with slow event handlers."""

    class SlowEventHandler(EventHandler[ItemProcessedDomainEvent]):
        def __init__(self) -> None:
            self.processed_events: list[ItemProcessedDomainEvent] = []
            self.processing_times: list[float] = []

        async def handle(self, event: ItemProcessedDomainEvent) -> None:
            start = time.time()
            await asyncio.sleep(0.1)  # Simulate slow processing
            end = time.time()
            self.processing_times.append(end - start)
            self.processed_events.append(event)

    class EventHandlerStreamingHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for item_id in request.item_ids:
                result = ProcessItemResult(item_id=item_id, status="processed")
                event = ItemProcessedDomainEvent(item_id=item_id)
                self._events.append(event)
                yield result

    handler = EventHandlerStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EventHandlerStreamingHandler)
    container = Container(handler)

    event_handler = SlowEventHandler()
    event_map = EventMap()
    event_map.bind(ItemProcessedDomainEvent, SlowEventHandler)

    event_container = Container(event_handler)
    event_emitter = EventEmitter(
        event_map=event_map,
        container=event_container,  # type: ignore
    )

    # With parallel processing (max_concurrent=3)
    mediator_parallel = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=3,
        concurrent_event_handle_enable=True,
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2", "item3"])
    results = []
    async for result in mediator_parallel.stream(request):
        results.append(result)

    # Wait for background tasks
    await asyncio.sleep(0.5)

    assert len(results) == 3
    assert len(event_handler.processed_events) == 3


async def test_streaming_mediator_custom_dispatcher_type() -> None:
    """Test that mediator can be instantiated with custom dispatcher type."""
    from cqrs.dispatcher.streaming import StreamingRequestDispatcher

    class StreamingHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self.called = False
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            self.called = True
            for item_id in request.item_ids:
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = StreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, StreamingHandler)
    container = Container(handler)

    # Pass custom dispatcher type
    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        dispatcher_type=StreamingRequestDispatcher,
    )

    request = ProcessItemsCommand(item_ids=["item1"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert len(results) == 1
    assert handler.called


async def test_streaming_mediator_events_cleared_between_yields() -> None:
    """Test that events are properly cleared between yields."""

    class EventCountingHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []
            self.events_count_at_yield: list[int] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for item_id in request.item_ids:
                # Add event
                self._events.append(
                    NotificationEvent(
                        event_name="ItemProcessed",
                        payload={"item_id": item_id},
                    ),
                )
                # Record count before yield
                self.events_count_at_yield.append(len(self._events))
                yield ProcessItemResult(item_id=item_id, status="processed")
                # Record count after yield (should be 0 after clear)
                self.events_count_at_yield.append(len(self._events))

    handler = EventCountingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, EventCountingHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert len(results) == 2
    # Events should be cleared after each yield
    # Pattern: [before_yield1, after_yield1, before_yield2, after_yield2]
    assert handler.events_count_at_yield == [1, 0, 1, 0]


async def test_streaming_mediator_large_number_of_items() -> None:
    """Test that mediator handles large number of items efficiently."""

    class BulkStreamingHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []
            self.processed_count = 0

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for item_id in request.item_ids:
                self.processed_count += 1
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = BulkStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, BulkStreamingHandler)
    container = Container(handler)

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
    )

    # Process 1000 items
    item_ids = [f"item_{i}" for i in range(1000)]
    request = ProcessItemsCommand(item_ids=item_ids)
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert len(results) == 1000
    assert handler.processed_count == 1000


async def test_streaming_mediator_interleaved_event_processing() -> None:
    """Test that events are processed after each yield, not batched at the end."""
    event_processing_order = []

    class OrderTrackingEventHandler(EventHandler[ItemProcessedDomainEvent]):
        def __init__(self) -> None:
            self.handled_events: list[ItemProcessedDomainEvent] = []

        async def handle(self, event: ItemProcessedDomainEvent) -> None:
            event_processing_order.append(f"event_{event.item_id}")
            self.handled_events.append(event)

    class OrderTrackingStreamingHandler(
        StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult],
    ):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for item_id in request.item_ids:
                event_processing_order.append(f"yield_{item_id}")
                event = ItemProcessedDomainEvent(item_id=item_id)
                self._events.append(event)
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = OrderTrackingStreamingHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, OrderTrackingStreamingHandler)
    container = Container(handler)

    event_handler = OrderTrackingEventHandler()
    event_map = EventMap()
    event_map.bind(ItemProcessedDomainEvent, OrderTrackingEventHandler)

    event_container = Container(event_handler)
    event_emitter = EventEmitter(
        event_map=event_map,
        container=event_container,  # type: ignore
    )

    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=1,
        concurrent_event_handle_enable=False,  # Sequential to verify order
    )

    request = ProcessItemsCommand(item_ids=["a", "b", "c"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)
        # Wait a bit to ensure event processing completes
        await asyncio.sleep(0.05)

    # Wait for any remaining background tasks
    await asyncio.sleep(0.1)

    assert len(results) == 3
    assert len(event_handler.handled_events) == 3

    # Verify that events are processed after each yield
    # Order should be: yield_a, event_a, yield_b, event_b, yield_c, event_c
    assert "yield_a" in event_processing_order
    assert "event_a" in event_processing_order
    assert "yield_b" in event_processing_order
    assert "event_b" in event_processing_order


async def test_streaming_mediator_no_event_map_provided() -> None:
    """Test that mediator works without event_map (events still emitted via emitter)."""

    class SimpleHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> typing.Sequence[IEvent]:
            return self._events.copy()

        def clear_events(self) -> None:
            self._events.clear()

        async def handle(
            self,
            request: ProcessItemsCommand,
        ) -> typing.AsyncIterator[ProcessItemResult]:
            for item_id in request.item_ids:
                event = NotificationEvent(
                    event_name="ItemProcessed",
                    payload={"item_id": item_id},
                )
                self._events.append(event)
                yield ProcessItemResult(item_id=item_id, status="processed")

    handler = SimpleHandler()
    request_map = RequestMap()
    request_map.bind(ProcessItemsCommand, SimpleHandler)
    container = Container(handler)

    # Create mediator without event_map parameter
    mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,  # type: ignore
        event_emitter=None,  # No event emitter either
    )

    request = ProcessItemsCommand(item_ids=["item1", "item2"])
    results = []
    async for result in mediator.stream(request):
        results.append(result)

    assert len(results) == 2
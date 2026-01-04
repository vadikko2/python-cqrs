import asyncio
from unittest import mock

import pydantic

from cqrs.events import (
    DomainEvent,
    EventEmitter,
    EventHandler,
    EventMap,
)
from cqrs.events.event_processor import EventProcessor


class _TestDomainEvent(DomainEvent, frozen=True):
    """Test domain event."""

    item_id: str = pydantic.Field()


class _TestEventHandler(EventHandler[_TestDomainEvent]):
    """Test event handler."""

    def __init__(self) -> None:
        self.processed_events: list[_TestDomainEvent] = []
        self.call_order: list[int] = []

    async def handle(self, event: _TestDomainEvent) -> None:
        """Handle event and track order."""
        self.processed_events.append(event)
        self.call_order.append(int(event.item_id))


class Container:
    """Simple container for testing."""

    def __init__(self, handler: _TestEventHandler) -> None:
        self._handler = handler

    async def resolve(self, type_) -> _TestEventHandler:
        """Resolve handler."""
        return self._handler


async def test_event_processor_processes_events_parallel() -> None:
    """Test that EventProcessor processes events in parallel when enabled."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
        _TestDomainEvent(item_id="3"),
    ]

    await processor.process_events(events)  # type: ignore[arg-type]

    assert len(event_handler.processed_events) == 3
    assert all(event in event_handler.processed_events for event in events)


async def test_event_processor_processes_events_sequentially() -> None:
    """Test that EventProcessor processes events sequentially when disabled."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=False,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
        _TestDomainEvent(item_id="3"),
    ]

    await processor.process_events(events)  # type: ignore[arg-type]

    assert len(event_handler.processed_events) == 3
    assert all(event in event_handler.processed_events for event in events)


async def test_event_processor_processes_empty_events_list() -> None:
    """Test that EventProcessor handles empty events list gracefully."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
    )

    await processor.process_events([])

    assert len(event_handler.processed_events) == 0


async def test_event_processor_emit_events_with_emitter() -> None:
    """Test that EventProcessor emits events via EventEmitter."""
    event_map = EventMap()
    container = Container(_TestEventHandler())

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock.AsyncMock()

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
    ]

    events_copy = events.copy()
    await processor.emit_events(events_copy)  # type: ignore[arg-type]  # type: ignore[arg-type]

    assert event_emitter.emit.call_count == 2
    # Check that events were passed to emit (order may vary due to pop)
    emitted_events = [call[0][0] for call in event_emitter.emit.call_args_list]
    assert events[0] in emitted_events
    assert events[1] in emitted_events
    # Events should be popped from the list (internal copy is modified)
    assert len(events_copy) == 0
    # Original list should remain unchanged
    assert len(events) == 2


async def test_event_processor_emit_events_without_emitter() -> None:
    """Test that EventProcessor does nothing when EventEmitter is None."""
    event_map = EventMap()
    container = Container(_TestEventHandler())

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        event_emitter=None,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
    ]

    # Should not raise any exception
    await processor.emit_events(events.copy())  # type: ignore[arg-type]  # type: ignore[arg-type]


async def test_event_processor_process_and_emit_events() -> None:
    """Test that process_and_emit_events combines processing and emitting."""
    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    event_emitter = mock.AsyncMock(spec=EventEmitter)
    event_emitter.emit = mock.AsyncMock()

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    events = [
        _TestDomainEvent(item_id="1"),
        _TestDomainEvent(item_id="2"),
    ]

    await processor.process_and_emit_events(events.copy())  # type: ignore[arg-type]  # type: ignore[arg-type]

    # Events should be processed
    assert len(event_handler.processed_events) == 2
    # Events should be emitted
    assert event_emitter.emit.call_count == 2
    # Original list should not be modified (copy is used internally)
    assert len(events) == 2


async def test_event_processor_respects_semaphore_limit() -> None:
    """Test that EventProcessor respects semaphore limit for concurrent processing."""
    # Track concurrent executions
    concurrent_count = 0
    max_concurrent = 0
    lock = asyncio.Lock()

    class TrackingEventHandler(EventHandler[_TestDomainEvent]):
        """Event handler that tracks concurrent executions."""

        def __init__(self) -> None:
            self.processed_events: list[_TestDomainEvent] = []

        async def handle(self, event: _TestDomainEvent) -> None:
            """Handle event and track concurrency."""
            nonlocal concurrent_count, max_concurrent
            async with lock:
                concurrent_count += 1
                max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # Small delay to allow concurrency
            async with lock:
                concurrent_count -= 1
            self.processed_events.append(event)

    event_handler = TrackingEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, TrackingEventHandler)
    container = Container(event_handler)  # type: ignore

    # Use semaphore limit of 2
    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    events = [_TestDomainEvent(item_id=str(i)) for i in range(5)]

    await processor.process_events(events)  # type: ignore[arg-type]

    # Max concurrent should not exceed semaphore limit (2)
    assert max_concurrent <= 2
    assert len(event_handler.processed_events) == 5


async def test_event_processor_with_middleware_chain() -> None:
    """Test that EventProcessor works with middleware chain."""
    from cqrs.middlewares.base import MiddlewareChain

    event_handler = _TestEventHandler()
    event_map = EventMap()
    event_map.bind(_TestDomainEvent, _TestEventHandler)
    container = Container(event_handler)

    middleware_chain = MiddlewareChain()

    processor = EventProcessor(
        event_map=event_map,
        container=container,  # type: ignore
        middleware_chain=middleware_chain,
    )

    events = [
        _TestDomainEvent(item_id="1"),
    ]

    await processor.process_events(events)  # type: ignore[arg-type]

    assert len(event_handler.processed_events) == 1

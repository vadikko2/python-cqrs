import asyncio
import typing

from cqrs.container.protocol import Container
from cqrs.dispatcher.event import EventDispatcher
from cqrs.events.event import Event
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.map import EventMap
from cqrs.middlewares.base import MiddlewareChain


class EventProcessor:
    """
    Processor for handling events in parallel or sequentially.

    Provides methods for processing events with semaphore limits and emitting
    them via event emitter. Can be reused across different mediators.
    """

    def __init__(
        self,
        event_map: EventMap,
        container: Container,
        event_emitter: EventEmitter | None = None,
        middleware_chain: MiddlewareChain | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
    ) -> None:
        """
        Initialize event processor.

        Args:
            event_map: Map of event types to handler types
            container: DI container for resolving event handlers
            event_emitter: Optional event emitter for publishing events
            middleware_chain: Optional middleware chain for event processing
            max_concurrent_event_handlers: Maximum number of concurrent event handlers
            concurrent_event_handle_enable: Whether to process events in parallel
        """
        self._event_emitter = event_emitter
        self._event_map = event_map
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._event_semaphore = asyncio.Semaphore(max_concurrent_event_handlers)
        self._event_dispatcher = EventDispatcher(
            event_map=event_map,
            container=container,
            middleware_chain=middleware_chain,
        )

    async def process_events(self, events: typing.List[Event]) -> None:
        """
        Process events in parallel (with semaphore limit) or sequentially.

        Args:
            events: List of events to process
        """
        if not events:
            return

        if not self._concurrent_event_handle_enable:
            # Process events sequentially
            for event in events:
                await self._event_dispatcher.dispatch(event)
        else:
            # Process events in parallel with semaphore limit
            tasks = [self._process_event_with_semaphore(event) for event in events]
            await asyncio.gather(*tasks)

    async def emit_events(self, events: typing.List[Event]) -> None:
        """
        Emit events via event emitter.

        Args:
            events: List of events to emit
        """
        if not self._event_emitter:
            return

        while events:
            event = events.pop()
            await self._event_emitter.emit(event)

    async def process_and_emit_events(self, events: typing.List[Event]) -> None:
        """
        Process events and then emit them.

        This is a convenience method that combines process_events and emit_events.

        Args:
            events: List of events to process and emit
        """
        events_copy = events.copy()
        await self.process_events(events_copy)
        await self.emit_events(events_copy)

    async def _process_event_with_semaphore(self, event: Event) -> None:
        """Process a single event with semaphore limit."""
        async with self._event_semaphore:
            await self._event_dispatcher.dispatch(event)

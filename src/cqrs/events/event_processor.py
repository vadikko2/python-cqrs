import asyncio
import typing

from cqrs.events.event import Event
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.map import EventMap


class EventProcessor:
    """
    Processor for handling events in parallel or sequentially.

    Provides methods for processing events with semaphore limits and emitting
    them via event emitter. Can be reused across different mediators.
    """

    def __init__(
            self,
            event_map: EventMap,
            event_emitter: EventEmitter | None = None,
            max_concurrent_event_handlers: int = 1,
            concurrent_event_handle_enable: bool = True,
    ) -> None:
        """
        Initialize event processor.

        Args:
            event_map: Map of event types to handler types
            event_emitter: Optional event emitter for publishing events
            max_concurrent_event_handlers: Maximum number of concurrent event handlers
            concurrent_event_handle_enable: Whether to process events in parallel
        """
        self._event_emitter = event_emitter
        self._event_map = event_map
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._event_semaphore = asyncio.Semaphore(max_concurrent_event_handlers)

    async def emit_events(self, events: typing.List[Event]) -> None:
        """
        Emit events via event emitter.

        Args:
            events: List of events to emit
        """
        if not events:
            return

        if not self._event_emitter:
            return

        if not self._concurrent_event_handle_enable:
            # Process events sequentially
            for event in events:
                await self._event_emitter.emit(event)
        else:
            # Process events in parallel with semaphore limit (fire-and-forget)
            for event in events:
                asyncio.create_task(self._emit_event_with_semaphore(event))

    async def _emit_event_with_semaphore(self, event: Event) -> None:
        """Process a single event with semaphore limit."""
        async with self._event_semaphore:
            await self._event_emitter.emit(event)

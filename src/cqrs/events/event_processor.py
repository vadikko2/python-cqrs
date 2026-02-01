import asyncio
import typing

from cqrs.events.event import IEvent
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.map import EventMap


class EventProcessor:
    """
    Processes events in parallel or sequentially via an event emitter.

    Emits events through the configured :class:`~cqrs.events.event_emitter.EventEmitter`.
    Follow-up events returned by handlers (via :attr:`~cqrs.events.event_handler.EventHandler.events`)
    are processed in the same pipeline: BFS in sequential mode, under the same
    semaphore in parallel mode. Can be reused across different mediators.

    Example::

        event_map = EventMap()
        event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)
        emitter = EventEmitter(event_map=event_map, container=container)
        processor = EventProcessor(
            event_map=event_map,
            event_emitter=emitter,
            max_concurrent_event_handlers=4,
            concurrent_event_handle_enable=True,
        )
        await processor.emit_events([OrderCreatedEvent(order_id="1")])
    """

    def __init__(
        self,
        event_map: EventMap,
        event_emitter: EventEmitter | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
    ) -> None:
        """
        Initialize the event processor.

        Args:
            event_map: Map of event types to handler types.
            event_emitter: Emitter used to publish events; if None, :meth:`emit_events`
                is a no-op.
            max_concurrent_event_handlers: Semaphore limit for parallel mode.
            concurrent_event_handle_enable: If True, process events in parallel
                (with semaphore); if False, process sequentially (BFS over events
                and follow-ups).
        """
        self._event_emitter = event_emitter
        self._event_map = event_map
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._event_semaphore = asyncio.Semaphore(max_concurrent_event_handlers)

    async def emit_events(self, events: typing.Sequence[IEvent]) -> None:
        """
        Emit all events and process follow-ups in the same pipeline.

        In sequential mode, events and follow-ups are processed in BFS order.
        In parallel mode, each event (and its follow-ups) is processed under the
        same semaphore limit. Returns when all work is scheduled; in parallel
        mode, handler tasks may still run after return.

        Args:
            events: Events to emit (e.g. domain events). Handlers may return
                follow-up events via :attr:`~cqrs.events.event_handler.EventHandler.events`.

        Example::

            await processor.emit_events([
                OrderCreatedEvent(order_id="1"),
                OrderCreatedEvent(order_id="2"),
            ])
        """
        if not events:
            return

        if not self._event_emitter:
            return

        if not self._concurrent_event_handle_enable:
            # Process events sequentially (BFS: follow-ups re-queued)
            to_process: list[IEvent] = list(events)
            while to_process:
                event = to_process.pop(0)
                follow_ups = await self._event_emitter.emit(event)
                to_process.extend(follow_ups)
        else:
            # Process events in parallel with semaphore limit (fire-and-forget)
            for event in events:
                asyncio.create_task(self._emit_event_with_semaphore(event))

    async def _emit_event_with_semaphore(self, event: IEvent) -> None:
        """
        Process one event under the semaphore and schedule follow-ups under the same semaphore.

        Args:
            event: The event to emit.
        """
        if not self._event_emitter:
            return
        async with self._event_semaphore:
            follow_ups = await self._event_emitter.emit(event)
            for follow_up in follow_ups:
                asyncio.create_task(self._emit_event_with_semaphore(follow_up))

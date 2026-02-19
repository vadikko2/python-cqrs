import functools
import logging
import typing

from cqrs import container as di_container, message_brokers
from cqrs.events.event import IDomainEvent, IEvent, INotificationEvent
from cqrs.events import event_handler, map

logger = logging.getLogger("cqrs")

_H: typing.TypeAlias = event_handler.EventHandler


class EventEmitter:
    """
    Sends events to registered handlers or to a message broker.

    For :class:`~cqrs.events.event.IDomainEvent`: resolves handlers from the
    container, runs :meth:`~cqrs.events.event_handler.EventHandler.handle`, and
    returns follow-up events from :attr:`~cqrs.events.event_handler.EventHandler.events`.
    For :class:`~cqrs.events.event.INotificationEvent`: sends the event to the
    message broker (if configured) and returns an empty sequence.
    """

    def __init__(
        self,
        event_map: map.EventMap,
        container: di_container.Container,
        message_broker: message_brokers.MessageBroker | None = None,
    ) -> None:
        """
        Initialize the event emitter.

        Args:
            event_map: Map of event types to handler types (used for domain events).
            container: DI container to resolve handler instances.
            message_broker: Optional broker for notification events; required
                when emitting :class:`~cqrs.events.event.INotificationEvent`.

        Example::

            event_map = EventMap()
            event_map.bind(OrderCreatedEvent, OrderCreatedEventHandler)
            emitter = EventEmitter(
                event_map=event_map,
                container=di_container,
                message_broker=kafka_broker,
            )
            follow_ups = await emitter.emit(OrderCreatedEvent(order_id="1"))
        """
        self._event_map = event_map
        self._container = container
        self._message_broker = message_broker

    @functools.singledispatchmethod
    async def emit(self, event: IEvent) -> typing.Sequence[IEvent]:
        """
        Emit an event and return follow-up events from handlers.

        For unknown event types returns an empty sequence. For domain events
        invokes all registered handlers and collects events from
        :attr:`~cqrs.events.event_handler.EventHandler.events`. For notification
        events sends to the message broker.

        Args:
            event: The event to emit (domain or notification).

        Returns:
            Follow-up events returned by domain event handlers; empty for
            notification events or when no handlers are registered.

        Example::

            follow_ups = await emitter.emit(OrderCreatedEvent(order_id="1"))
            for e in follow_ups:
                await emitter.emit(e)  # or process via EventProcessor
        """
        return ()

    async def _send_to_broker(
        self,
        event: INotificationEvent,
    ) -> None:
        """
        Send a notification event to the message broker.

        Args:
            event: Notification event to send.

        Raises:
            RuntimeError: If no message broker was configured.
        """
        if not self._message_broker:
            raise RuntimeError(
                f"To send event {event}, message broker must be specified.",
            )

        message = message_brokers.Message(
            message_name=type(event).__name__,
            message_id=event.event_id,
            topic=event.topic,
            payload=event.to_dict(),
        )

        logger.debug(
            "Sending Event(%s) to message broker %s",
            event.event_id,
            type(self._message_broker).__name__,
        )

        await self._message_broker.send_message(message)

    @emit.register(IDomainEvent)
    async def _(self, event: IDomainEvent) -> typing.Sequence[IEvent]:
        """Emit domain event: run all registered handlers and return their follow-up events."""
        handlers_types = self._event_map.get(type(event), [])
        if not handlers_types:
            logger.warning(
                "Handlers for domain event %s not found",
                type(event).__name__,
            )
            return ()
        follow_ups: list[IEvent] = []
        for handler_type in handlers_types:
            handler: _H = await self._container.resolve(
                handler_type,
            )
            logger.debug(
                "Handling Event(%s) via event handler(%s)",
                type(event).__name__,
                handler_type.__name__,
            )
            await handler.handle(event)
            # Snapshot follow-ups so shared handlers don't expose stale state
            follow_ups.extend(list(handler.events))
        return follow_ups

    @emit.register(INotificationEvent)
    async def _(self, event: INotificationEvent) -> typing.Sequence[IEvent]:
        """Emit notification event: send to message broker; no follow-ups."""
        await self._send_to_broker(event)
        return ()

import asyncio
import functools
import logging
import typing

from cqrs import container as di_container, message_brokers
from cqrs.events import event as event_model, event_handler, map

logger = logging.getLogger("cqrs")

_H: typing.TypeAlias = event_handler.EventHandler | event_handler.SyncEventHandler


class EventEmitter:
    """
    The event emitter is responsible for sending events to the according handlers or
    to the message broker abstraction.
    """

    def __init__(
        self,
        event_map: map.EventMap,
        container: di_container.Container,
        message_broker: message_brokers.MessageBroker | None = None,
    ) -> None:
        self._event_map = event_map
        self._container = container
        self._message_broker = message_broker

    @functools.singledispatchmethod
    async def emit(self, event: event_model.Event) -> None:
        pass

    async def _send_to_broker(
        self,
        event: event_model.NotificationEvent | event_model.ECSTEvent,
    ) -> None:
        """
        Sends event to the message broker.
        """
        if not self._message_broker:
            raise RuntimeError(
                f"To use {event.event_type}, message_broker argument must be specified.",
            )

        message = message_brokers.Message(
            message_type=event.event_type,
            message_name=type(event).__name__,
            message_id=event.event_id,
            topic=event.topic,
            payload=event.model_dump(mode="json"),
        )

        logger.debug(
            "Sending %s Event(%s) to message broker %s",
            event.event_type,
            event.event_id,
            type(self._message_broker).__name__,
        )

        await self._message_broker.send_message(message)

    @emit.register
    async def _(self, event: event_model.DomainEvent) -> None:
        handlers_types = self._event_map.get(type(event), [])
        if not handlers_types:
            logger.warning(
                "Handlers for domain event %s not found",
                type(event).__name__,
            )
        for handler_type in handlers_types:
            handler: _H = await self._container.resolve(
                handler_type,
            )
            logger.debug(
                "Handling Event(%s) via event handler(%s)",
                type(event).__name__,
                handler_type.__name__,
            )
            if asyncio.iscoroutinefunction(handler.handle):
                await handler.handle(event)
            else:
                await asyncio.to_thread(handler.handle, event)

    @emit.register
    async def _(self, event: event_model.NotificationEvent) -> None:
        await self._send_to_broker(event)

    @emit.register
    async def _(self, event: event_model.ECSTEvent) -> None:
        await self._send_to_broker(event)

import functools
import logging

from cqrs import container, message_brokers
from cqrs.events import event, map

logger = logging.getLogger("cqrs")


class EventEmitter:
    """
    The event emitter is responsible for sending events to the according handlers or
    to the message broker abstraction.
    """

    def __init__(
        self,
        event_map: map.EventMap,
        container: container.Container,
        message_broker: message_brokers.MessageBroker | None = None,
    ) -> None:
        self._event_map = event_map
        self._container = container
        self._message_broker = message_broker

    @functools.singledispatchmethod
    async def emit(self, event: event.Event) -> None: ...

    @emit.register
    async def _(self, event: event.DomainEvent) -> None:
        handlers_types = self._event_map.get(type(event), [])
        if not handlers_types:
            logger.warning(
                "Handlers for domain event %s not found",
                type(event).__name__,
            )
        for handler_type in handlers_types:
            handler = await self._container.resolve(handler_type)
            logger.debug(
                "Handling Event(%s) via event handler(%s)",
                type(event).__name__,
                handler_type.__name__,
            )
            await handler.handle(event)

    @emit.register
    async def _(self, event: event.NotificationEvent) -> None:
        if not self._message_broker:
            raise RuntimeError(
                "To use NotificationEvent, message_broker argument must be specified.",
            )

        message = _build_message(event)

        logger.debug(
            "Sending Notification Event(%s) to message broker %s",
            event.event_id,
            type(self._message_broker).__name__,
        )

        await self._message_broker.send_message(message)

    @emit.register
    async def _(self, event: event.ECSTEvent) -> None:
        if not self._message_broker:
            raise RuntimeError(
                "To use ECSTEvent, message_broker argument must be specified.",
            )

        message = _build_message(event)

        logger.debug(
            "Sending ECST event(%s) to message broker %s",
            event.event_id,
            type(self._message_broker).__name__,
        )

        await self._message_broker.send_message(message)


def _build_message(
    event: event.NotificationEvent | event.ECSTEvent,
) -> message_brokers.Message:
    payload = event.model_dump(mode="json")

    return message_brokers.Message(
        message_type=event.event_type,
        message_name=type(event).__name__,
        message_id=event.event_id,
        payload=payload,
    )

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
    async def emit(self, event: IEvent) -> None:
        pass

    async def _send_to_broker(
        self,
        event: INotificationEvent,
    ) -> None:
        """
        Sends event to the message broker.
        """
        if not self._message_broker:
            raise RuntimeError(
                f"To send event {event}, message_broker argument must be specified.",
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

    @emit.register
    async def _(self, event: IDomainEvent) -> None:
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
            await handler.handle(event)

    @emit.register
    async def _(self, event: INotificationEvent) -> None:
        await self._send_to_broker(event)

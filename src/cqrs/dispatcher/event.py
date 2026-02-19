import logging
import typing

from cqrs.container.protocol import Container
from cqrs.events.event import IEvent
from cqrs.events.event_handler import EventHandler
from cqrs.events.map import EventMap
from cqrs.middlewares.base import MiddlewareChain

_EventHandler: typing.TypeAlias = EventHandler

logger = logging.getLogger("cqrs")


class EventDispatcher:
    def __init__(
        self,
        event_map: EventMap,
        container: Container,
        middleware_chain: MiddlewareChain | None = None,
    ) -> None:
        self._event_map = event_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()

    async def _handle_event(
        self,
        event: IEvent,
        handle_type: typing.Type[_EventHandler],
    ) -> None:
        handler: _EventHandler = await self._container.resolve(handle_type)
        await handler.handle(event)
        for follow_up in handler.events:
            await self.dispatch(follow_up)

    async def dispatch(self, event: IEvent) -> None:
        handler_types = self._event_map.get(type(event), [])
        if not handler_types:
            logger.warning(
                "Handlers for event %s not found",
                type(event).__name__,
            )
            return
        for h_type in handler_types:
            await self._handle_event(event, h_type)

import logging
import typing

import pydantic

from cqrs import (
    container as di_container,
    events as cqrs_events,
    middlewares,
    requests,
    response as res,
)

logger = logging.getLogger("cqrs")

Resp = typing.TypeVar("Resp", res.Response, None, contravariant=True)


class RequestHandlerDoesNotExist(Exception): ...


class RequestDispatchResult(pydantic.BaseModel, typing.Generic[Resp]):
    response: Resp = pydantic.Field(default=None)
    events: typing.List[cqrs_events.Event] = pydantic.Field(default_factory=list)


class RequestDispatcher:
    def __init__(
        self,
        request_map: requests.RequestMap,
        container: di_container.Container,
        middleware_chain: middlewares.MiddlewareChain | None = None,
    ) -> None:
        self._request_map = request_map
        self._container = container
        self._middleware_chain = middleware_chain or middlewares.MiddlewareChain()

    async def dispatch(self, request: requests.Request) -> RequestDispatchResult:
        handler_type = self._request_map.get(type(request), None)
        if handler_type is None:
            raise RequestHandlerDoesNotExist(
                f"RequestHandler not found matching Request type {type(request)}",
            )
        handler = await self._container.resolve(handler_type)
        wrapped_handle = self._middleware_chain.wrap(handler.handle)
        response = await wrapped_handle(request)
        return RequestDispatchResult(response=response, events=handler.events)


E = typing.TypeVar("E", bound=cqrs_events.Event, contravariant=True)


class EventDispatcher:
    def __init__(
        self,
        event_map: cqrs_events.EventMap,
        container: di_container.Container,
        middleware_chain: middlewares.MiddlewareChain | None = None,
    ):
        self._event_map = event_map
        self._container = container
        self._middleware_chain = middleware_chain or middlewares.MiddlewareChain()

    async def _handle_event(
        self,
        event: cqrs_events.Event,
        handle_type: typing.Type[cqrs_events.EventHandler[cqrs_events.Event]],
    ):
        handler = await self._container.resolve(handle_type)
        await handler.handle(event)

    async def dispatch(self, event: cqrs_events.Event) -> None:
        handler_types = self._event_map.get(type(event), [])
        if not handler_types:
            logger.warning(
                "Handlers for event %s not found",
                type(event).__name__,
            )
        for h_type in handler_types:
            await self._handle_event(event, h_type)

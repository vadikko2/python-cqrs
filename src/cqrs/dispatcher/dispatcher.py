import asyncio
import functools
import logging
import typing

import pydantic

from cqrs import (
    container as di_container,
    events as cqrs_events,
    middlewares,
    requests,
    response as resp,
)
from cqrs.events import event_handler
from cqrs.requests import request_handler

logger = logging.getLogger("cqrs")

_Resp = typing.TypeVar("_Resp", resp.Response, None, contravariant=True)

_RequestHandler: typing.TypeAlias = (
    request_handler.RequestHandler | request_handler.SyncRequestHandler
)
_EventHandler: typing.TypeAlias = (
    event_handler.EventHandler | event_handler.SyncEventHandler
)


class RequestHandlerDoesNotExist(Exception):
    pass


class RequestDispatchResult(pydantic.BaseModel, typing.Generic[_Resp]):
    response: _Resp = pydantic.Field(default=None)
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
        handler: _RequestHandler = await self._container.resolve(handler_type)
        if asyncio.iscoroutinefunction(handler.handle):
            wrapped_handle = self._middleware_chain.wrap(handler.handle)
        else:
            wrapped_handle = self._middleware_chain.wrap(
                functools.partial(asyncio.to_thread, handler.handle),
            )
        response = await wrapped_handle(request)

        return RequestDispatchResult(response=response, events=handler.events)


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
        handle_type: typing.Type[cqrs_events.EventHandler],
    ):
        handler: _EventHandler = await self._container.resolve(handle_type)
        if asyncio.iscoroutinefunction(handler.handle):
            await handler.handle(event)
        else:
            await asyncio.to_thread(handler.handle, event)

    async def dispatch(self, event: cqrs_events.Event) -> None:
        handler_types = self._event_map.get(type(event), [])
        if not handler_types:
            logger.warning(
                "Handlers for event %s not found",
                type(event).__name__,
            )
        for h_type in handler_types:
            await self._handle_event(event, h_type)

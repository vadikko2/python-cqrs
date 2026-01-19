import asyncio
import logging
import typing
from collections import abc

from cqrs.container.protocol import Container
from cqrs.dispatcher.exceptions import (
    RequestHandlerDoesNotExist,
    RequestHandlerTypeError,
)
from cqrs.dispatcher.models import RequestDispatchResult
from cqrs.middlewares.base import MiddlewareChain
from cqrs.requests.cor_request_handler import (
    CORRequestHandler,
    build_chain,
    CORRequestHandlerT as CORRequestHandlerType,
)
from cqrs.requests.map import RequestMap, HandlerType
from cqrs.requests.request import IRequest
from cqrs.requests.request_handler import RequestHandler

logger = logging.getLogger("cqrs")

_RequestHandler: typing.TypeAlias = RequestHandler | CORRequestHandler


class RequestDispatcher:
    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        middleware_chain: MiddlewareChain | None = None,
    ) -> None:
        self._request_map = request_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()

    async def _resolve_handler(
        self,
        handler_type: HandlerType,
    ) -> _RequestHandler:
        """
        Resolve a single handler or build a chain from multiple handlers.

        For single handlers, resolves them using the DI container.
        For lists of handlers, validates they are COR handlers and builds a chain.
        """
        if isinstance(handler_type, abc.Iterable):
            if not all(
                issubclass(
                    handler_cls,
                    CORRequestHandler,
                )
                for handler_cls in handler_type
            ):
                raise RequestHandlerTypeError(
                    "COR handler must be type CORRequestHandler",
                )

            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(self._container.resolve(h)) for h in handler_type
                ]
            handlers = [task.result() for task in tasks]
            return build_chain(
                typing.cast(typing.List[CORRequestHandlerType], handlers),
            )

        return typing.cast(_RequestHandler, await self._container.resolve(handler_type))

    async def dispatch(self, request: IRequest) -> RequestDispatchResult:
        handler_type = self._request_map.get(type(request), None)
        if not handler_type:
            raise RequestHandlerDoesNotExist(
                f"RequestHandler not found matching Request type {type(request)}",
            )
        handler: _RequestHandler = await self._resolve_handler(handler_type)
        wrapped_handle = self._middleware_chain.wrap(handler.handle)
        response = await wrapped_handle(request)

        return RequestDispatchResult(response=response, events=handler.events)

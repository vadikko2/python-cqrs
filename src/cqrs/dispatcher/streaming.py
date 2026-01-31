import inspect
import typing

from cqrs.container.protocol import Container
from cqrs.dispatcher.exceptions import RequestHandlerDoesNotExist
from cqrs.dispatcher.models import RequestDispatchResult
from cqrs.middlewares.base import MiddlewareChain
from cqrs.requests.map import RequestMap
from cqrs.requests.request import IRequest
from cqrs.requests.request_handler import StreamingRequestHandler


class StreamingRequestDispatcher:
    """
    Dispatcher for streaming request handlers that work as generators.

    This dispatcher handles requests using handlers that yield responses
    as generators. After each yield, events are collected and can be emitted.
    """

    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        middleware_chain: MiddlewareChain | None = None,
    ) -> None:
        self._request_map = request_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()

    def dispatch(
        self,
        request: IRequest,
    ) -> typing.AsyncIterator[RequestDispatchResult]:
        """
        Dispatch a request to a streaming handler and yield results.

        Called without await; returns an AsyncIterator consumed with async for.
        After each yield from the handler, events are collected and included
        in the dispatch result. The generator continues until StopIteration.
        """
        return self._dispatch_impl(request)

    async def _dispatch_impl(
        self,
        request: IRequest,
    ) -> typing.AsyncIterator[RequestDispatchResult]:
        handler_type = self._request_map.get(type(request), None)
        if handler_type is None:
            raise RequestHandlerDoesNotExist(
                f"StreamingRequestHandler not found matching Request type {type(request)}",
            )

        # Streaming dispatcher only works with streaming handlers, not lists
        if isinstance(handler_type, list):
            raise TypeError(
                "StreamingRequestDispatcher does not support COR handler chains. "
                "Use RequestDispatcher for chain of responsibility pattern.",
            )

        # Type narrowing: handler_type is now a single handler type
        handler_type_typed = typing.cast(
            typing.Type[StreamingRequestHandler],
            handler_type,
        )
        handler: StreamingRequestHandler = await self._container.resolve(
            handler_type_typed,
        )

        if not inspect.isasyncgenfunction(handler.handle):
            handler_name = (
                handler_type_typed.__name__
                if hasattr(handler_type_typed, "__name__")
                else str(handler_type_typed)
            )
            raise TypeError(
                f"Handler {handler_name}.handle must be an async generator function",
            )

        async_gen = handler.handle(request)
        async for response in async_gen:
            events = list(handler.events)
            handler.clear_events()
            yield RequestDispatchResult(
                response=response,
                events=events,
            )

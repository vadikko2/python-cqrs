import asyncio
import typing

from cqrs.container.protocol import Container
from cqrs.dispatcher.event import EventDispatcher
from cqrs.dispatcher.request import RequestDispatcher
from cqrs.dispatcher.streaming import StreamingRequestDispatcher
from cqrs.events.event import Event
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.map import EventMap
from cqrs.middlewares.base import MiddlewareChain
from cqrs.requests.map import RequestMap
from cqrs.requests.request import Request
from cqrs.response import Response

_ResponseT = typing.TypeVar("_ResponseT", Response, None, covariant=True)


class RequestMediator:
    """
    The request mediator object.

    Handles requests and processes events. Events can be processed in parallel
    (with semaphore limit) or sequentially depending on concurrent_event_handle_enable.

    Usage::

      message_broker = AMQPMessageBroker(
        dsn=f"amqp://{LOGIN}:{PASSWORD}@{HOSTNAME}/",
        queue_name="user_joined_domain",
        exchange_name="user_joined",
      )
      event_map = EventMap()
      event_map.bind(UserJoinedDomainEvent, UserJoinedDomainEventHandler)
      request_map = RequestMap()
      request_map.bind(JoinUserCommand, JoinUserCommandHandler)
      event_emitter = EventEmitter(event_map, container, message_broker)

      mediator = RequestMediator(
        request_map=request_map,
        container=container,
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
      )

      # Handles command, processes events (in parallel or sequentially),
      # and publishes events via event emitter.
      await mediator.send(join_user_command)

    """

    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        event_emitter: EventEmitter | None = None,
        middleware_chain: MiddlewareChain | None = None,
        event_map: EventMap | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
        *,
        dispatcher_type: typing.Type[RequestDispatcher] = RequestDispatcher,
    ) -> None:
        self._event_emitter = event_emitter
        self._event_map = event_map or EventMap()
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._event_semaphore = asyncio.Semaphore(max_concurrent_event_handlers)
        self._event_dispatcher = EventDispatcher(
            event_map=self._event_map,
            container=container,
            middleware_chain=middleware_chain,
        )
        self._dispatcher = dispatcher_type(
            request_map=request_map,  # type: ignore
            container=container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
        )

    async def send(self, request: Request) -> _ResponseT:
        """
        Send a request and return the response.

        The return type is inferred from the request type based on the handler
        registered in the RequestMap. For proper type inference, ensure your
        RequestHandler is properly typed with RequestHandler[RequestType, ResponseType].

        Note: TypeVar usage here is intentional for type inference purposes.
        """
        dispatch_result = await self._dispatcher.dispatch(request)

        if dispatch_result.events:
            await self._process_events_parallel(dispatch_result.events.copy())
            await self._send_events(dispatch_result.events.copy())

        return dispatch_result.response

    async def _process_event_with_semaphore(self, event: Event) -> None:
        """Process a single event with semaphore limit."""
        async with self._event_semaphore:
            await self._event_dispatcher.dispatch(event)

    async def _process_events_parallel(
        self,
        events: typing.List[Event],
    ) -> None:
        """Process events in parallel with semaphore limit or sequentially."""
        if not events:
            return

        if not self._concurrent_event_handle_enable:
            # Process events sequentially
            for event in events:
                await self._event_dispatcher.dispatch(event)
        else:
            # Process events in parallel with semaphore limit
            tasks = [self._process_event_with_semaphore(event) for event in events]
            await asyncio.gather(*tasks)

    async def _send_events(self, events: typing.List[Event]) -> None:
        if not self._event_emitter:
            return

        while events:
            event = events.pop()
            await self._event_emitter.emit(event)


class EventMediator:
    """
    The event mediator object.

    Usage::
      event_map = EventMap()
      event_map.bind(UserJoinedECSTEvent, UserJoinedECSTEventHandler)
      mediator = EventMediator(
        event_map=event_map,
        container=container
      )

      # Handles ecst and notification events.
      await mediator.send(user_joined_event)
    """

    def __init__(
        self,
        event_map: EventMap,
        container: Container,
        middleware_chain: MiddlewareChain | None = None,
        *,
        dispatcher_type: typing.Type[EventDispatcher] = EventDispatcher,
    ):
        self._dispatcher = dispatcher_type(
            event_map=event_map,  # type: ignore
            container=container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
        )

    async def send(self, event: Event) -> None:
        await self._dispatcher.dispatch(event)


class StreamingRequestMediator:
    """
    The streaming request mediator object.

    This mediator works with handlers that are generators. It processes requests
    by iterating through the generator, emitting events after each yield, and
    streaming results back to the client.

    Usage::

      message_broker = AMQPMessageBroker(
        dsn=f"amqp://{LOGIN}:{PASSWORD}@{HOSTNAME}/",
        queue_name="user_joined_domain",
        exchange_name="user_joined",
      )
      event_map = EventMap()
      event_map.bind(UserJoinedDomainEvent, UserJoinedDomainEventHandler)
      request_map = RequestMap()
      request_map.bind(ProcessItemsCommand, ProcessItemsCommandHandler)
      event_emitter = EventEmitter(event_map, container, message_broker)

      mediator = StreamingRequestMediator(
        request_map=request_map,
        container=container,
        event_emitter=event_emitter,
      )

      # Streams results and publishes events after each yield.
      async for result in mediator.stream(process_items_command):
          print(f"Processed: {result.response}")
    """

    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        event_emitter: EventEmitter | None = None,
        middleware_chain: MiddlewareChain | None = None,
        event_map: EventMap | None = None,
        max_concurrent_event_handlers: int = 1,
        concurrent_event_handle_enable: bool = True,
        *,
        dispatcher_type: typing.Type[
            StreamingRequestDispatcher
        ] = StreamingRequestDispatcher,
    ) -> None:
        self._event_emitter = event_emitter
        self._event_map = event_map or EventMap()
        self._max_concurrent_event_handlers = max_concurrent_event_handlers
        self._concurrent_event_handle_enable = concurrent_event_handle_enable
        self._event_semaphore = asyncio.Semaphore(max_concurrent_event_handlers)
        self._event_dispatcher = EventDispatcher(
            event_map=self._event_map,
            container=container,
            middleware_chain=middleware_chain,
        )
        self._dispatcher = dispatcher_type(
            request_map=request_map,  # type: ignore
            container=container,  # type: ignore
            middleware_chain=middleware_chain,  # type: ignore
        )

    async def stream(
        self,
        request: Request,
    ) -> typing.AsyncIterator[Response | None]:
        """
        Stream results from a generator-based handler.

        After each yield from the handler:
        1. Events are processed (in parallel with semaphore limit or sequentially
           depending on concurrent_event_handle_enable) via event dispatcher
        2. Events are emitted via the event emitter
        3. The response is yielded to the client

        The generator continues until StopIteration is raised.
        """
        async for dispatch_result in self._dispatcher.dispatch(request):
            if dispatch_result.events:
                await self._process_events_parallel(dispatch_result.events.copy())
                await self._send_events(dispatch_result.events.copy())

            yield dispatch_result.response

    async def _process_event_with_semaphore(self, event: Event) -> None:
        """Process a single event with semaphore limit."""
        async with self._event_semaphore:
            await self._event_dispatcher.dispatch(event)

    async def _process_events_parallel(self, events: typing.List[Event]) -> None:
        """Process events in parallel with semaphore limit or sequentially."""
        if not events:
            return

        if not self._concurrent_event_handle_enable:
            # Process events sequentially
            for event in events:
                await self._event_dispatcher.dispatch(event)
        else:
            # Process events in parallel with semaphore limit
            tasks = [self._process_event_with_semaphore(event) for event in events]
            await asyncio.gather(*tasks)

    async def _send_events(self, events: typing.List[Event]) -> None:
        if not self._event_emitter:
            return

        while events:
            event = events.pop()
            await self._event_emitter.emit(event)

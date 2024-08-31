import typing

import di

import cqrs
from cqrs import events, requests
from cqrs.container import di as di_container_impl
from cqrs.message_brokers import devnull, protocol
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware

DEFAULT_MESSAGE_BROKER = devnull.DevnullMessageBroker()


def setup_event_emitter(
    container: di_container_impl.DIContainer,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    message_broker: protocol.MessageBroker | None = None,
):
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    event_mapper = events.EventMap()

    if domain_events_mapper:
        domain_events_mapper(event_mapper)

    return events.EventEmitter(
        event_map=event_mapper,
        container=container,
        message_broker=message_broker,
    )


def setup_mediator(
    event_emitter: events.EventEmitter,
    container: di_container_impl.DIContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    commands_mapper: typing.Callable[[requests.RequestMap], None] | None = None,
    queries_mapper: typing.Callable[[requests.RequestMap], None] | None = None,
) -> cqrs.RequestMediator:
    requests_mapper = requests.RequestMap()
    if commands_mapper:
        commands_mapper(requests_mapper)
    if queries_mapper:
        queries_mapper(requests_mapper)

    middleware_chain = mediator_middlewares.MiddlewareChain()

    for middleware in middlewares:
        middleware_chain.add(middleware)

    return cqrs.RequestMediator(
        request_map=requests_mapper,
        container=container,
        event_emitter=event_emitter,
        middleware_chain=middleware_chain,
    )


def bootstrap(
    di_container: di.Container,
    message_broker: protocol.MessageBroker | None = None,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    commands_mapper: typing.Callable[[requests.RequestMap], None] | None = None,
    domain_events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    queries_mapper: typing.Callable[[requests.RequestMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
) -> cqrs.RequestMediator:
    if message_broker is None:
        message_broker = DEFAULT_MESSAGE_BROKER
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    container = di_container_impl.DIContainer()
    container.attach_external_container(di_container)

    event_emitter = setup_event_emitter(
        container,
        domain_events_mapper,
        message_broker,
    )
    middlewares_list: typing.List[mediator_middlewares.Middleware] = list(
        middlewares or [],
    )
    return setup_mediator(
        event_emitter,
        container,
        middlewares=middlewares_list + [logging_middleware.LoggingMiddleware()],
        commands_mapper=commands_mapper,
        queries_mapper=queries_mapper,
    )

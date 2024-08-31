import typing

import di

import cqrs
from cqrs import events
from cqrs.container import di as di_container_impl
from cqrs.middlewares import base as mediator_middlewares, logging as logging_middleware


def setup_mediator(
    container: di_container_impl.DIContainer,
    middlewares: typing.Iterable[mediator_middlewares.Middleware],
    events_mapper: typing.Callable[[events.EventMap], None] | None = None,
) -> cqrs.EventMediator:
    _events_mapper = events.EventMap()
    if events_mapper is not None:
        events_mapper(_events_mapper)

    middleware_chain = mediator_middlewares.MiddlewareChain()

    for middleware in middlewares:
        middleware_chain.add(middleware)

    return cqrs.EventMediator(
        event_map=_events_mapper,
        container=container,
        middleware_chain=middleware_chain,
    )


def bootstrap(
    di_container: di.Container,
    middlewares: typing.Sequence[mediator_middlewares.Middleware] | None = None,
    events_mapper: typing.Callable[[events.EventMap], None] | None = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
) -> cqrs.EventMediator:
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    container = di_container_impl.DIContainer()
    container.attach_external_container(di_container)
    middlewares_list: typing.List[mediator_middlewares.Middleware] = list(
        middlewares or [],
    )
    return setup_mediator(
        container,
        events_mapper=events_mapper,
        middlewares=middlewares_list + [logging_middleware.LoggingMiddleware()],
    )

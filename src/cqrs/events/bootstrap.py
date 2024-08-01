import typing

import di

import cqrs
from cqrs import events
from cqrs.container import di as ed_di_container
from cqrs.middlewares import base as mediator_middlewares
from cqrs.middlewares import logging as logging_middleware


def setup_mediator(
    container: ed_di_container.DIContainer | None,
    middlewares: typing.Iterable[mediator_middlewares.Middleware] | None = None,
    events_mapper: typing.Callable[[events.EventMap], None] = None,
) -> cqrs.EventMediator:

    _events_mapper = events.EventMap()
    if events_mapper:
        events_mapper(_events_mapper)

    middleware_chain = mediator_middlewares.MiddlewareChain()
    if middlewares is None:
        middlewares = []

    for middleware in middlewares:
        middleware_chain.add(middleware)

    return cqrs.EventMediator(
        event_map=_events_mapper,
        container=container,
        middleware_chain=middleware_chain,
    )


def bootstrap(
    di_container: di.Container | None = None,
    middlewares: typing.Iterable[mediator_middlewares.Middleware] | None = None,
    events_mapper: typing.Callable[[events.EventMap], None] = None,
    on_startup: typing.List[typing.Callable[[], None]] | None = None,
) -> cqrs.EventMediator:
    if on_startup is None:
        on_startup = []

    for fun in on_startup:
        fun()

    if middlewares is None:
        middlewares = []
    container = ed_di_container.DIContainer(di_container)
    return setup_mediator(
        container,
        events_mapper=events_mapper,
        middlewares=middlewares + [logging_middleware.LoggingMiddleware()],
    )

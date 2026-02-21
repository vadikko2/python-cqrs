"""Tests for RequestHandlerFallback (without circuit breaker)."""

from typing import Any, TypeVar

import pytest

from cqrs import RequestHandlerFallback
from cqrs.container.protocol import Container
from cqrs.dispatcher import RequestDispatcher
from cqrs.events.event import IEvent
from cqrs.requests.map import RequestMap
from cqrs.requests.request import Request
from cqrs.requests.request_handler import RequestHandler
from cqrs.response import Response

T = TypeVar("T")


class SimpleCommand(Request):
    value: str


class SimpleResult(Response):
    value: str


class PrimaryHandler(RequestHandler[SimpleCommand, SimpleResult]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []
        self.called = False

    @property
    def events(self) -> list[IEvent]:
        return self._events.copy()

    async def handle(self, request: SimpleCommand) -> SimpleResult:
        self.called = True
        raise RuntimeError("Primary failed")


class FallbackHandler(RequestHandler[SimpleCommand, SimpleResult]):
    def __init__(self) -> None:
        self._events: list[IEvent] = []
        self.called = False

    @property
    def events(self) -> list[IEvent]:
        return self._events.copy()

    async def handle(self, request: SimpleCommand) -> SimpleResult:
        self.called = True
        return SimpleResult(value=f"fallback:{request.value}")


class _TestRequestContainer:
    """Minimal container for request fallback tests; implements Container protocol."""

    def __init__(self) -> None:
        self._primary = PrimaryHandler()
        self._fallback = FallbackHandler()
        self._external_container: Any = None

    @property
    def external_container(self) -> Any:
        return self._external_container

    def attach_external_container(self, container: Any) -> None:
        self._external_container = container

    async def resolve(self, type_: type[T]) -> T:
        if type_ is PrimaryHandler:
            return self._primary  # type: ignore[return-value]
        if type_ is FallbackHandler:
            return self._fallback  # type: ignore[return-value]
        raise KeyError(type_)


@pytest.mark.asyncio
async def test_request_fallback_no_cb_primary_fails_uses_fallback() -> None:
    request_map: RequestMap = RequestMap()
    request_map.bind(
        SimpleCommand,
        RequestHandlerFallback(PrimaryHandler, FallbackHandler),
    )
    container: Container[Any] = _TestRequestContainer()
    dispatcher = RequestDispatcher(request_map=request_map, container=container)

    result = await dispatcher.dispatch(SimpleCommand(value="x"))

    assert result.response.value == "fallback:x"
    assert container._primary.called
    assert container._fallback.called


@pytest.mark.asyncio
async def test_request_fallback_failure_exceptions_only_matching_triggers_fallback() -> None:
    request_map = RequestMap()
    request_map.bind(
        SimpleCommand,
        RequestHandlerFallback(
            PrimaryHandler,
            FallbackHandler,
            failure_exceptions=(ValueError,),
        ),
    )
    container: Container[Any] = _TestRequestContainer()
    dispatcher = RequestDispatcher(request_map=request_map, container=container)

    with pytest.raises(RuntimeError, match="Primary failed"):
        await dispatcher.dispatch(SimpleCommand(value="x"))

    assert container._primary.called
    assert not container._fallback.called

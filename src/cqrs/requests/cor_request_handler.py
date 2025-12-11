from __future__ import annotations
import abc
import functools
import typing

from cqrs import response
from cqrs.events import event
from cqrs.requests import request as r

_Req = typing.TypeVar("_Req", bound=r.Request, contravariant=True)
_Resp = typing.TypeVar("_Resp", response.Response, None, covariant=True)


class CORRequestHandler(abc.ABC, typing.Generic[_Req, _Resp]):
    """
    The chain of responsibility request handler interface.

    Implements the chain of responsibility pattern, allowing multiple handlers
    to process a request in sequence until one handles it successfully.

    Chain handler example::

      class AuthenticationHandler(CORRequestHandler[LoginCommand, None]):
          def __init__(self, auth_service: AuthServiceProtocol) -> None:
              self._auth_service = auth_service
              self.events: typing.List[Event] = []

          async def handle(self, request: LoginCommand) -> None | None:
              if self._auth_service.can_authenticate(request):
                  return await self._auth_service.authenticate(request)
              return await super().handle(request)
    """

    _next_handler: "CORRequestHandler" = None

    def set_next(self, handler: "CORRequestHandler") -> "CORRequestHandler":
        if self._next_handler is None:
            self._next_handler = handler

        return self._next_handler

    async def next(self, request: _Req) -> _Resp:
        if self._next_handler:
            return await self._next_handler.handle(request)

        return None

    @property
    @abc.abstractmethod
    def events(self) -> typing.List[event.Event]:
        raise NotImplementedError

    @abc.abstractmethod
    async def handle(self, request: _Req) -> _Resp:
        raise NotImplementedError


class SyncCORRequestHandler(abc.ABC, typing.Generic[_Req, _Resp]):
    """
    The synchronous chain of responsibility request handler interface.

    Implements the chain of responsibility pattern, allowing multiple handlers
    to process a request in sequence until one handles it successfully.

    Chain handler example::

      class AuthenticationHandler(SyncCORRequestHandler[LoginCommand, None]):
          def __init__(self, auth_service: AuthServiceProtocol) -> None:
              self._auth_service = auth_service
              self.events: typing.List[Event] = []

          def handle(self, request: LoginCommand) -> None | None:
              if self._auth_service.can_authenticate(request):
                  return self._auth_service.authenticate(request)
              return super().handle(request)
    """

    _next_handler: "SyncCORRequestHandler" = None

    def set_next(self, handler: "SyncCORRequestHandler") -> "SyncCORRequestHandler":
        if self._next_handler is None:
            self._next_handler = handler

        return self._next_handler

    def next(self, request: _Req) -> _Resp:
        if self._next_handler:
            return self._next_handler.handle(request)

        return None

    @property
    @abc.abstractmethod
    def events(self) -> typing.List[event.Event]:
        raise NotImplementedError

    @abc.abstractmethod
    def handle(self, request: _Req) -> _Resp:
        raise NotImplementedError


_RequestHandler: typing.TypeAlias = CORRequestHandler | SyncCORRequestHandler


def build_chain(handlers: typing.List[_RequestHandler]) -> _RequestHandler:
    """
    Build a chain of responsibility from a list of handlers.

    Links handlers together in the order they appear in the list,
    with each handler pointing to the next one in sequence.

    Args:
        handlers: List of handlers to be linked together

    Returns:
        The first handler in the chain
    """

    def link(handler1, handler2):
        handler1.set_next(handler2)
        return handler2

    functools.reduce(link, handlers)
    return handlers[0]

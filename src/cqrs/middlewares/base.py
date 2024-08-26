import functools
import typing

from cqrs import requests, response

Req = typing.TypeVar("Req", bound=requests.Request, contravariant=True)
Res = typing.TypeVar("Res", response.Response, None, covariant=True)
HandleType = typing.Callable[[Req], typing.Awaitable[Res]]


class Middleware(typing.Protocol):
    async def __call__(self, request: requests.Request, handle: HandleType) -> Res: ...


class MiddlewareChain:
    def __init__(self) -> None:
        self._chain: list[Middleware] = []

    def set(self, chain: list[Middleware]) -> None:
        self._chain = chain

    def add(self, middleware: Middleware) -> None:
        self._chain.append(middleware)

    def wrap(self, handle: HandleType) -> HandleType:
        for middleware in reversed(self._chain):
            handle = functools.partial(middleware.__call__, handle=handle)

        return handle

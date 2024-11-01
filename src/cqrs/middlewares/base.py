import functools
import typing

from cqrs import requests, response

_Req = typing.TypeVar("_Req", bound=requests.Request, contravariant=True)
_Res = typing.TypeVar("_Res", response.Response, None, covariant=True)
HandleType = typing.Callable[[_Req], typing.Awaitable[_Res] | _Res]


class Middleware(typing.Protocol[_Req, _Res]):
    async def __call__(self, request: _Req, handle: HandleType) -> _Res:
        raise NotImplementedError


class MiddlewareChain:
    def __init__(self) -> None:
        self._chain: typing.List[Middleware] = []

    def set(self, chain: typing.List[Middleware]) -> None:
        self._chain = chain

    def add(self, middleware: Middleware) -> None:
        self._chain.append(middleware)

    def wrap(self, handle: HandleType) -> HandleType:
        for middleware in reversed(self._chain):
            handle = functools.partial(middleware.__call__, handle=handle)

        return handle

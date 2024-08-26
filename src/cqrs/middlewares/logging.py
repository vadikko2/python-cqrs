import logging
import typing

from cqrs import requests, response
from cqrs.middlewares import base

Req = typing.TypeVar("Req", bound=requests.Request, contravariant=True)
Res = typing.TypeVar("Res", response.Response, None, covariant=True)
HandleType = typing.Callable[[Req], typing.Awaitable[Res]]

logger = logging.getLogger("cqrs")


class LoggingMiddleware(base.Middleware):
    async def __call__(self, request: requests.Request, handle: HandleType) -> Res:
        logger.debug(
            "Handle %s request",
            type(request).__name__,
            extra={
                "request_json_fields": {"request": request.model_dump(mode="json")},
                "to_mask": True,
            },
        )
        resp = await handle(request)
        logger.debug(
            "Request %s handled",
            type(request).__name__,
            extra={
                "request_json_fields": {
                    "response": resp.model_dump(mode="json") if resp else {},
                },
                "to_mask": True,
            },
        )

        return resp

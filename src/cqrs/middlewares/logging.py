import logging

from cqrs.middlewares import base
from cqrs.middlewares.base import HandleType
from cqrs.requests.request import Request
from cqrs.response import Response

logger = logging.getLogger("cqrs")


class LoggingMiddleware(base.Middleware):
    async def __call__(self, request: Request, handle: HandleType) -> Response | None:
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

import logging
import typing

import pydantic

from cqrs.events.event import Event
from cqrs.response import Response

logger = logging.getLogger("cqrs")

_ResponseT = typing.TypeVar("_ResponseT", Response, None, covariant=True)


class RequestDispatchResult(pydantic.BaseModel, typing.Generic[_ResponseT]):
    response: _ResponseT = pydantic.Field(default=None)
    events: typing.List[Event] = pydantic.Field(default_factory=list)

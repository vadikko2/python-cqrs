import dataclasses
import logging
import typing

from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga.step import SagaStepResult

logger = logging.getLogger("cqrs")

_ResponseT = typing.TypeVar("_ResponseT", Response, None, covariant=True)


@dataclasses.dataclass
class RequestDispatchResult(typing.Generic[_ResponseT]):
    """Result of request dispatch execution."""

    response: _ResponseT
    events: typing.List[Event] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class SagaDispatchResult:
    """Result of saga dispatch execution for a single step."""

    step_result: SagaStepResult
    events: typing.List[Event] = dataclasses.field(default_factory=list)
    saga_id: str | None = None

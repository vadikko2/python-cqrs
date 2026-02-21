"""Fallback wrapper for request handlers with optional circuit breaker."""

import dataclasses

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.requests.request_handler import RequestHandler, StreamingRequestHandler

RequestHandlerT = type[RequestHandler] | type[StreamingRequestHandler]


@dataclasses.dataclass(frozen=True)
class RequestHandlerFallback:
    """
    Fallback wrapper for request handlers.

    When the primary handler fails (or circuit breaker is open), the fallback
    handler is invoked. Use a separate circuit breaker instance per domain
    (e.g. one for requests) that uses the same adapter class.

    Attributes:
        primary: The primary request handler class.
        fallback: The fallback handler class to execute if primary fails.
        failure_exceptions: Exception types that trigger fallback; if empty, any exception.
        circuit_breaker: Optional circuit breaker instance (e.g. AioBreakerAdapter).

    Example::
        request_cb = AioBreakerAdapter(fail_max=5, timeout_duration=60)
        request_map.bind(
            MyCommand,
            RequestHandlerFallback(
                MyCommandHandler,
                MyCommandHandlerFallback,
                failure_exceptions=(ConnectionError, TimeoutError),
                circuit_breaker=request_cb,
            ),
        )
    """

    primary: RequestHandlerT
    fallback: RequestHandlerT
    failure_exceptions: tuple[type[Exception], ...] = ()
    circuit_breaker: ICircuitBreaker | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.primary, type) or not isinstance(self.fallback, type):
            raise TypeError(
                "RequestHandlerFallback primary and fallback must be handler classes",
            )
        primary_streaming = issubclass(self.primary, StreamingRequestHandler)
        fallback_streaming = issubclass(self.fallback, StreamingRequestHandler)
        if primary_streaming != fallback_streaming:
            raise TypeError(
                "RequestHandlerFallback primary and fallback must be the same handler base type: "
                "both RequestHandler or both StreamingRequestHandler",
            )

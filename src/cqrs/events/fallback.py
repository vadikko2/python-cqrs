"""Fallback wrapper for event handlers with optional circuit breaker."""

import dataclasses
import typing

from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.events import event_handler

EventHandlerT = typing.Type[event_handler.EventHandler]


@dataclasses.dataclass(frozen=True)
class EventHandlerFallback:
    """
    Fallback wrapper for event handlers.

    When the primary handler fails (or circuit breaker is open), the fallback
    handler is invoked. Use a separate circuit breaker instance per domain
    (e.g. one for events) that uses the same adapter class.

    Attributes:
        primary: The primary event handler class.
        fallback: The fallback handler class to execute if primary fails.
        failure_exceptions: Exception types that trigger fallback; if empty, any exception.
        circuit_breaker: Optional circuit breaker instance (e.g. AioBreakerAdapter).

    Example::
        event_cb = AioBreakerAdapter(fail_max=5, timeout_duration=60)
        event_map.bind(
            OrderCreatedEvent,
            EventHandlerFallback(
                SendEmailHandler,
                SendEmailFallbackHandler,
                circuit_breaker=event_cb,
            ),
        )
    """

    primary: EventHandlerT
    fallback: EventHandlerT
    failure_exceptions: tuple[type[Exception], ...] = ()
    circuit_breaker: ICircuitBreaker | None = None

"""Unified circuit breaker protocol for Saga, Request and Event fallbacks."""

import typing


class ICircuitBreaker(typing.Protocol):
    """
    Unified interface for circuit breaker implementations.

    Used by Saga step fallbacks, Request handler fallbacks and Event handler
    fallbacks. Each fallback type typically uses its own adapter instance;
    the same adapter class works for all with identifier-based namespacing.

    Attributes:
        identifier: Handler/step type or string used as circuit breaker namespace.
    """

    async def call(
        self,
        identifier: type | str,
        func: typing.Callable[..., typing.Awaitable[typing.Any]],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        """
        Execute the function with circuit breaker protection.

        Args:
            identifier: Handler/step type or string for breaker namespace.
            func: The async function to execute.
            *args: Positional arguments to pass to func.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            The result of func execution.

        Raises:
            CircuitBreakerError: If the circuit breaker is open.
            Exception: Any exception raised by func (if circuit is closed).
        """
        ...

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        """
        Check if the given exception is a circuit breaker error.

        Args:
            exc: The exception to check.

        Returns:
            True if the exception is a circuit breaker error, False otherwise.
        """
        ...

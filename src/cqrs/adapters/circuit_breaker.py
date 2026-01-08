"""Circuit breaker adapter for aiobreaker integration."""

import logging
import typing
from datetime import timedelta

from cqrs.saga.circuit_breaker import ISagaStepCircuitBreaker
from cqrs.saga.step import SagaStepHandler

logger = logging.getLogger("cqrs.adapters.circuit_breaker")

try:
    import aiobreaker
    from aiobreaker import CircuitBreaker, CircuitBreakerError

    try:
        from aiobreaker import CircuitBreakerListener
    except ImportError:
        # Fallback if CircuitBreakerListener is not available
        CircuitBreakerListener = object  # type: ignore[assignment, misc]
except ImportError:
    aiobreaker = None  # type: ignore[assignment, misc]
    CircuitBreaker = None  # type: ignore[assignment, misc]
    CircuitBreakerError = None  # type: ignore[assignment, misc]
    CircuitBreakerListener = object  # type: ignore[assignment, misc]

    logger.warning(
        "aiobreaker is not installed. Install it with: pip install aiobreaker",
    )


class CriticalLogListener(CircuitBreakerListener if aiobreaker else object):  # type: ignore[misc]
    """
    Listener for circuit breaker state changes with critical logging.

    Implements CircuitBreakerListener interface methods.
    """

    def __init__(self, breaker_name: str):
        self._breaker_name = breaker_name

    def before_call(
        self,
        breaker: typing.Any,
        func: typing.Any,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> None:
        """Called before the circuit breaker calls the function."""
        pass

    def failure(self, breaker: typing.Any, exception: Exception) -> None:
        """Called when the function fails."""
        pass

    def success(self, breaker: typing.Any) -> None:
        """Called when the function succeeds."""
        pass

    def state_change(
        self,
        breaker: typing.Any,
        old: typing.Any,
        new: typing.Any,
    ) -> None:
        """Log circuit breaker state changes at critical level."""
        logger.critical(
            f"{self._breaker_name} circuit breaker state changed: {old.state} -> {new.state}",
        )


def circuit_breaker_storage_factory() -> typing.Any:
    """
    Factory function to create in-memory circuit breaker storage.

    Returns:
        CircuitMemoryStorage instance.

    Raises:
        ImportError: If aiobreaker is not installed.
    """
    if aiobreaker is None:
        raise ImportError(
            "aiobreaker is not installed. Install it with: pip install aiobreaker",
        )

    try:
        from aiobreaker.storage.memory import CircuitMemoryStorage
    except ImportError:
        raise ImportError(
            "Memory storage requires aiobreaker. " "Make sure aiobreaker is installed.",
        )

    return CircuitMemoryStorage(state=aiobreaker.CircuitBreakerState.CLOSED)


class AioBreakerAdapter(ISagaStepCircuitBreaker):
    """
    Adapter for aiobreaker circuit breaker with in-memory storage.

    Manages circuit breaker instances per step type. Each step type gets its own
    isolated circuit breaker with a namespace based on the step class name.

    Attributes:
        fail_max: Maximum number of failures before opening the circuit.
        timeout_duration: Time to wait before attempting to reset the circuit (in seconds).
        exclude: List of exception types that should NOT open the circuit
                (business exceptions).

    Example::
        adapter = AioBreakerAdapter(
            fail_max=3,
            timeout_duration=60,
            exclude=[InventoryOutOfStockError]
        )
    """

    def __init__(
        self,
        fail_max: int = 5,
        timeout_duration: int = 60,
        exclude: list[type[Exception]] | None = None,
    ) -> None:
        if CircuitBreaker is None:
            raise ImportError(
                "aiobreaker is not installed. Install it with: pip install aiobreaker",
            )

        self._fail_max = fail_max
        self._timeout_duration = timeout_duration
        self._exclude = exclude or []

        # Dictionary to store circuit breakers per step type
        self._breakers: dict[str, typing.Any] = {}  # type: ignore[type-arg]

    def _get_step_name(self, step_type: type[SagaStepHandler]) -> str:
        """
        Get name for a step type.

        Uses the fully qualified name of the step class as namespace.

        Args:
            step_type: The step handler class type.

        Returns:
            Name string for the circuit breaker (module.class_name).
        """
        module = getattr(step_type, "__module__", "")
        name = step_type.__name__
        return f"{module}.{name}" if module else name

    def _get_breaker(self, step_type: type[SagaStepHandler]) -> typing.Any:  # type: ignore[return-type]
        """
        Get or create circuit breaker for a step type.

        Each step type gets its own isolated circuit breaker with a unique name
        based on the step's fully qualified name.

        Args:
            step_type: The step handler class type.

        Returns:
            CircuitBreaker instance for this step type.
        """
        step_name = self._get_step_name(step_type)

        if step_name not in self._breakers:
            breaker = self._create_breaker(step_name)
            self._breakers[step_name] = breaker

        return self._breakers[step_name]

    def _create_breaker(self, name: str) -> typing.Any:  # type: ignore[return-type]
        """
        Create circuit breaker for a step.

        For each step, creates a breaker with in-memory storage.
        Each step gets its own isolated circuit breaker instance.

        Args:
            name: Name for the circuit breaker (step's fully qualified name).

        Returns:
            CircuitBreaker instance.
        """
        if CircuitBreaker is None:
            raise ImportError(
                "aiobreaker is not installed. Install it with: pip install aiobreaker",
            )

        # Create in-memory storage for this step
        # Each step gets its own storage instance for proper isolation
        storage = circuit_breaker_storage_factory()

        listeners: list[typing.Any] = [CriticalLogListener(name)]

        return CircuitBreaker(
            state_storage=storage,
            name=name,
            fail_max=self._fail_max,
            timeout_duration=timedelta(seconds=self._timeout_duration),
            exclude=self._exclude,
            listeners=listeners,
        )

    async def call(
        self,
        step_type: type[SagaStepHandler],
        func: typing.Callable[..., typing.Awaitable[typing.Any]],
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> typing.Any:
        """
        Execute the function with circuit breaker protection.

        Args:
            step_type: The step handler class type. Used to determine breaker name.
            func: The async function to execute.
            *args: Positional arguments to pass to func.
            **kwargs: Keyword arguments to pass to func.

        Returns:
            The result of func execution.

        Raises:
            CircuitBreakerError: If the circuit breaker is open.
            Exception: Any exception raised by func (if circuit is closed).
        """
        breaker = self._get_breaker(step_type)
        return await breaker.call_async(func, *args, **kwargs)

    def is_circuit_breaker_error(self, exc: Exception) -> bool:
        """
        Check if the given exception is a circuit breaker error.

        Args:
            exc: The exception to check.

        Returns:
            True if the exception is a CircuitBreakerError, False otherwise.
        """
        if CircuitBreakerError is None:
            return False
        return isinstance(exc, CircuitBreakerError)

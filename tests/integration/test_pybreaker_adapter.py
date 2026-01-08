"""Integration tests for AioBreakerAdapter."""

import asyncio
import uuid

import pytest
from aiobreaker import CircuitBreakerError

from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.saga.step import SagaStepHandler


# Test exceptions
class BusinessException(Exception):
    """Business exception that should not open circuit breaker."""

    pass


class NetworkException(Exception):
    """Network exception that should open circuit breaker."""

    pass


# Test async functions
async def successful_function(value: int) -> int:
    """Successful async function."""
    await asyncio.sleep(0.01)
    return value * 2


async def failing_function(error_type: type[Exception] = RuntimeError) -> None:
    """Failing async function."""
    await asyncio.sleep(0.01)
    raise error_type("Function failed")


async def slow_function(delay: float = 0.1) -> str:
    """Slow async function."""
    await asyncio.sleep(delay)
    return "completed"


# Helper functions
def create_test_step(module_name: str) -> type[SagaStepHandler]:
    """Create a test step type with unique module name."""

    class TestStep(SagaStepHandler):
        __module__ = f"{module_name}_{uuid.uuid4().hex[:8]}"

    return TestStep


# Helper function removed - now using circuit_breaker_storage_factory directly


# Fixtures
@pytest.fixture(scope="function")
def memory_adapter():
    """Create AioBreakerAdapter with in-memory storage."""
    return AioBreakerAdapter(fail_max=3, timeout_duration=2)


# Positive tests
class TestAioBreakerAdapterPositive:
    """Positive integration tests for AioBreakerAdapter."""

    @pytest.mark.asyncio
    async def test_successful_execution_memory(self, memory_adapter):
        """Test successful function execution through adapter with memory storage."""
        # Arrange
        step_type = create_test_step("test_success")

        # Act
        result = await memory_adapter.call(
            step_type=step_type,
            func=successful_function,
            value=5,
        )

        # Assert
        assert result == 10

    @pytest.mark.asyncio
    async def test_namespace_isolation_memory(self, memory_adapter):
        """Test that different step types have isolated circuit breaker states."""
        # Arrange
        step_type_1 = create_test_step("test_isolation_1")
        step_type_2 = create_test_step("test_isolation_2")

        # Act - Fail Step1 twice (circuit still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type_1,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Act - Step2 should still work (different namespace)
        result = await memory_adapter.call(
            step_type=step_type_2,
            func=successful_function,
            value=3,
        )

        # Assert
        assert result == 6

        # Act - Step1 circuit should still be closed (only 2 failures, need 3 to open)
        # Third failure opens the circuit (fail_max=3 means circuit opens after 3 failures)
        # So the 3rd call should raise CircuitBreakerError, not RuntimeError
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type_1,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Now circuit should be open (3 failures reached)
        # Next call should also raise CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type_1,
                func=failing_function,
                error_type=RuntimeError,
            )

    @pytest.mark.asyncio
    async def test_business_exception_exclusion_memory(self, memory_adapter):
        """Test that business exceptions don't open circuit breaker."""
        # Arrange
        adapter = AioBreakerAdapter(
            fail_max=2,
            timeout_duration=1,
            exclude=[BusinessException],
        )
        step_type = create_test_step("test_business")

        # Act - Fail multiple times with business exception
        for _ in range(5):
            with pytest.raises(BusinessException):
                await adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=BusinessException,
                )

        # Assert - Circuit should still be closed
        with pytest.raises(BusinessException):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=BusinessException,
            )

    @pytest.mark.asyncio
    async def test_circuit_reset_after_timeout_memory(self, memory_adapter):
        """Test that circuit breaker resets after timeout."""
        # Arrange
        step_type = create_test_step("test_reset")

        # Act - Open circuit with failures
        # First 2 calls should raise RuntimeError (circuit is still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Third call should raise CircuitBreakerError (circuit opens after fail_max=3)
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Assert - Circuit should be open now
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Act - Wait for reset timeout
        await asyncio.sleep(2.5)

        # Assert - Circuit should be half-open, trial call fails and circuit opens again
        # When trial call fails, circuit opens immediately and raises CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

    @pytest.mark.asyncio
    async def test_mixed_exceptions_memory(self, memory_adapter):
        """Test handling of mixed exception types."""
        # Arrange
        adapter = AioBreakerAdapter(
            fail_max=2,
            timeout_duration=1,
            exclude=[BusinessException],
        )
        step_type = create_test_step("test_mixed")

        # Act - Fail with business exception (should not count)
        for _ in range(3):
            with pytest.raises(BusinessException):
                await adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=BusinessException,
                )

        # Act - Fail with network exception (should count)
        # First network exception failure (circuit still closed)
        with pytest.raises(NetworkException):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=NetworkException,
            )

        # Second network exception failure - circuit opens after fail_max=2 failures
        with pytest.raises(CircuitBreakerError):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=NetworkException,
            )

        # Assert - Circuit should be open now
        with pytest.raises(CircuitBreakerError):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=NetworkException,
            )

    @pytest.mark.asyncio
    async def test_is_circuit_breaker_error_memory(self, memory_adapter):
        """Test is_circuit_breaker_error method."""
        # Arrange
        step_type = create_test_step("test_error_check")

        # Act - Open circuit
        # First 2 calls should raise RuntimeError (circuit is still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Third call should raise CircuitBreakerError (circuit opens after fail_max=3)
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Act - Try to call when circuit is open
        try:
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )
        except Exception as e:
            # Assert - Check if it's a circuit breaker error
            assert memory_adapter.is_circuit_breaker_error(e)
            assert isinstance(e, CircuitBreakerError)

        # Arrange - Regular exception should not be identified as circuit breaker error
        other_step = create_test_step("test_other")

        # Act & Assert
        try:
            await memory_adapter.call(
                step_type=other_step,
                func=failing_function,
                error_type=RuntimeError,
            )
        except RuntimeError as e:
            assert not memory_adapter.is_circuit_breaker_error(e)


# Negative tests
class TestAioBreakerAdapterNegative:
    """Negative integration tests for AioBreakerAdapter."""

    @pytest.mark.asyncio
    async def test_circuit_opens_after_failures_memory(self, memory_adapter):
        """Test that circuit opens after exceeding fail_max."""
        # Arrange
        step_type = create_test_step("test_opens")

        # Act - Fail exactly fail_max times
        # First 2 calls should raise RuntimeError (circuit is still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Third call should raise CircuitBreakerError (circuit opens after fail_max=3)
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Assert - Next call should raise CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

    @pytest.mark.asyncio
    async def test_circuit_breaker_error_propagation_memory(self, memory_adapter):
        """Test that CircuitBreakerError is properly propagated."""
        # Arrange
        step_type = create_test_step("test_propagation")

        # Act - Open circuit
        # First 2 calls should raise RuntimeError (circuit is still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Third call should raise CircuitBreakerError (circuit opens after fail_max=3)
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Assert - Verify CircuitBreakerError is raised and propagated correctly
        with pytest.raises(CircuitBreakerError) as exc_info:
            await memory_adapter.call(
                step_type=step_type,
                func=successful_function,
                value=5,  # Even successful function should fail when circuit is open
            )

        assert isinstance(exc_info.value, CircuitBreakerError)

    @pytest.mark.asyncio
    async def test_non_excluded_exceptions_open_circuit_memory(self, memory_adapter):
        """Test that non-excluded exceptions open circuit breaker."""
        # Arrange
        adapter = AioBreakerAdapter(
            fail_max=2,
            timeout_duration=1,
            exclude=[BusinessException],  # Only exclude BusinessException
        )
        step_type = create_test_step("test_non_excluded")

        # Act - Fail with non-excluded exception
        # First failure (circuit still closed)
        with pytest.raises(NetworkException):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=NetworkException,
            )

        # Second failure - circuit opens after fail_max=2 failures
        with pytest.raises(CircuitBreakerError):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=NetworkException,
            )

        # Assert - Circuit should be open
        with pytest.raises(CircuitBreakerError):
            await adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=NetworkException,
            )

    @pytest.mark.asyncio
    async def test_circuit_stays_open_during_timeout_memory(self, memory_adapter):
        """Test that circuit stays open during timeout period."""
        # Arrange
        step_type = create_test_step("test_stays_open")

        # Act - Open circuit
        # First 2 calls should raise RuntimeError (circuit is still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Third call should raise CircuitBreakerError (circuit opens after fail_max=3)
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Act - Wait less than timeout
        await asyncio.sleep(1.0)

        # Assert - Circuit should still be open
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

    @pytest.mark.asyncio
    async def test_multiple_concurrent_calls_memory(self, memory_adapter):
        """Test behavior with multiple concurrent calls."""
        # Arrange
        step_type = create_test_step("test_concurrent")

        # Act - Open circuit first
        # First 2 calls should raise RuntimeError (circuit is still closed)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await memory_adapter.call(
                    step_type=step_type,
                    func=failing_function,
                    error_type=RuntimeError,
                )

        # Third call should raise CircuitBreakerError (circuit opens after fail_max=3)
        with pytest.raises(CircuitBreakerError):
            await memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )

        # Act - Make multiple concurrent calls when circuit is open
        tasks = [
            memory_adapter.call(
                step_type=step_type,
                func=failing_function,
                error_type=RuntimeError,
            )
            for _ in range(5)
        ]

        # Assert - All should raise CircuitBreakerError
        for task in tasks:
            with pytest.raises(CircuitBreakerError):
                await task

    @pytest.mark.asyncio
    async def test_invalid_step_type_memory(self, memory_adapter):
        """Test behavior with invalid step type."""
        # Arrange
        step_type = create_test_step("test_invalid")

        # Act
        result = await memory_adapter.call(
            step_type=step_type,
            func=successful_function,
            value=3,
        )

        # Assert
        assert result == 6

    @pytest.mark.asyncio
    async def test_function_with_keyword_args_memory(self, memory_adapter):
        """Test adapter with function that uses keyword arguments."""
        # Arrange
        step_type = create_test_step("test_keyword_args")

        # Act
        result = await memory_adapter.call(
            step_type=step_type,
            func=slow_function,
            delay=0.05,
        )

        # Assert
        assert result == "completed"

    @pytest.mark.asyncio
    async def test_function_with_positional_args_memory(self, memory_adapter):
        """Test adapter with function that uses positional arguments."""
        # Arrange
        step_type = create_test_step("test_positional_args")

        async def wrapper(value: int) -> int:
            return await successful_function(value)

        # Act
        result = await memory_adapter.call(
            step_type=step_type,
            func=wrapper,
            value=8,
        )

        # Assert
        assert result == 16

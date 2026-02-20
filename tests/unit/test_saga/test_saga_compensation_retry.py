"""Tests for compensation retry mechanism."""

from unittest.mock import patch

import pytest

from cqrs.saga.saga import Saga
from cqrs.saga.storage.memory import MemorySagaStorage

from .conftest import (
    AlwaysFailingCompensationStep,
    CompensationRetryStep,
    FailingStep,
    OrderContext,
    ReserveInventoryStep,
    SagaContainer,
)


async def test_compensation_retry_succeeds_after_failures(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retries and succeeds after initial failures."""
    reserve_step = ReserveInventoryStep()
    retry_step = CompensationRetryStep(fail_count=2)

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(CompensationRetryStep, retry_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, CompensationRetryStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError, match="Step failed for order 123"):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
            compensation_retry_count=3,
            compensation_retry_delay=0.1,
        ) as transaction:
            async for _ in transaction:
                pass

    # Compensation should have been called 3 times (2 failures + 1 success)
    assert retry_step.compensate_called_count == 3
    assert reserve_step.compensate_called


async def test_compensation_retry_exhausts_all_attempts(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retries all configured attempts before giving up."""
    reserve_step = ReserveInventoryStep()
    always_failing_step = AlwaysFailingCompensationStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(AlwaysFailingCompensationStep, always_failing_step)

    retry_count = 5

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, AlwaysFailingCompensationStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError, match="Step failed for order 123"):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
            compensation_retry_count=retry_count,
            compensation_retry_delay=0.01,
        ) as transaction:
            async for _ in transaction:
                pass

    # Compensation should have been called exactly retry_count times
    assert always_failing_step.compensate_called_count == retry_count
    assert reserve_step.compensate_called


async def test_compensation_retry_uses_exponential_backoff(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retry uses exponential backoff for delays."""
    retry_step = CompensationRetryStep(fail_count=3)

    saga_container.register(CompensationRetryStep, retry_step)

    initial_delay = 0.1
    backoff_multiplier = 2.0
    retry_count = 4

    class TestSaga(Saga[OrderContext]):
        steps = [CompensationRetryStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    sleep_times = []

    async def mock_sleep(delay: float) -> None:
        """Mock sleep that records delay without actually sleeping."""
        sleep_times.append(delay)
        # Return immediately without actual sleep

    # Patch asyncio.sleep in the compensation module where it's used
    with patch("cqrs.saga.compensation.asyncio.sleep", side_effect=mock_sleep):
        with pytest.raises(ValueError):
            async with saga.transaction(
                context=context,
                container=saga_container,  # type: ignore
                storage=MemorySagaStorage(),
                compensation_retry_count=retry_count,
                compensation_retry_delay=initial_delay,
                compensation_retry_backoff=backoff_multiplier,
            ) as transaction:
                async for _ in transaction:
                    pass

    # Should have retried 4 times (3 failures + 1 success)
    assert retry_step.compensate_called_count == 4

    # Check that delays follow exponential backoff pattern
    # Attempt 1 fails -> wait initial_delay * (backoff^0) = 0.1
    # Attempt 2 fails -> wait initial_delay * (backoff^1) = 0.2
    # Attempt 3 fails -> wait initial_delay * (backoff^2) = 0.4
    # Attempt 4 succeeds -> no wait
    expected_delays = [initial_delay * (backoff_multiplier**i) for i in range(3)]

    assert len(sleep_times) == 3, f"Expected 3 sleep calls, got {len(sleep_times)}: {sleep_times}"
    for actual, expected in zip(sleep_times, expected_delays):
        assert abs(actual - expected) < 0.01, f"Expected {expected}, got {actual}"


async def test_compensation_retry_configurable_parameters(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retry parameters are configurable."""
    retry_step = CompensationRetryStep(fail_count=1)

    saga_container.register(CompensationRetryStep, retry_step)

    # Test with custom retry configuration
    custom_retry_count = 5
    custom_delay = 0.05
    custom_backoff = 3.0

    class TestSaga(Saga[OrderContext]):
        steps = [CompensationRetryStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
            compensation_retry_count=custom_retry_count,
            compensation_retry_delay=custom_delay,
            compensation_retry_backoff=custom_backoff,
        ) as transaction:
            async for _ in transaction:
                pass

    # Should succeed after 2 attempts (1 failure + 1 success)
    assert retry_step.compensate_called_count == 2


async def test_compensation_retry_default_parameters(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retry uses default parameters when not specified."""
    retry_step = CompensationRetryStep(fail_count=1)

    saga_container.register(CompensationRetryStep, retry_step)

    class TestSaga(Saga[OrderContext]):
        steps = [CompensationRetryStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
        ) as transaction:
            async for _ in transaction:
                pass

    # Should succeed after 2 attempts with default retry count of 3
    assert retry_step.compensate_called_count == 2


async def test_compensation_retry_multiple_steps(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retry works correctly for multiple steps."""

    # Create separate step classes to avoid instance conflicts
    class RetryStep1(CompensationRetryStep):
        pass

    class RetryStep2(CompensationRetryStep):
        pass

    reserve_step = ReserveInventoryStep()
    retry_step1_instance = RetryStep1(fail_count=1)
    retry_step2_instance = RetryStep2(fail_count=2)

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(RetryStep1, retry_step1_instance)
    saga_container.register(RetryStep2, retry_step2_instance)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, RetryStep1, RetryStep2, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
            compensation_retry_count=3,
            compensation_retry_delay=0.01,
        ) as transaction:
            async for _ in transaction:
                pass

    # Both retry steps should have been compensated with retries
    assert retry_step1_instance.compensate_called_count == 2  # 1 failure + 1 success
    assert retry_step2_instance.compensate_called_count == 3  # 2 failures + 1 success
    assert reserve_step.compensate_called


async def test_compensation_retry_does_not_mask_original_error(
    saga_container: SagaContainer,
) -> None:
    """Test that compensation retry failures don't mask the original error."""
    always_failing_step = AlwaysFailingCompensationStep()

    saga_container.register(AlwaysFailingCompensationStep, always_failing_step)

    class TestSaga(Saga[OrderContext]):
        steps = [AlwaysFailingCompensationStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    # Original error should still be raised even if all compensation retries fail
    with pytest.raises(ValueError, match="Step failed for order 123"):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
            compensation_retry_count=2,
            compensation_retry_delay=0.01,
        ) as transaction:
            async for _ in transaction:
                pass

    # Compensation should have been attempted
    assert always_failing_step.compensate_called_count == 2

"""Tests for saga recovery functionality."""

import typing
import uuid
from unittest.mock import AsyncMock

import pytest

from cqrs.saga.recovery import recover_saga
from cqrs.saga.saga import Saga
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage

from .conftest import (
    FailingStep,
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


async def test_recover_saga_successfully_resumes_running_saga(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga successfully resumes a running saga."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    # Create a saga in storage with RUNNING status
    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    # Mark first step as completed
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )

    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()
    ship_step = ShipOrderStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)
    saga_container.register(ShipOrderStep, ship_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    saga = TestSaga()

    # Recover the saga
    await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # First step should be skipped (already completed)
    assert not reserve_step.act_called

    # Remaining steps should be executed
    assert payment_step.act_called
    assert ship_step.act_called

    # Verify saga is completed
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.COMPLETED


async def test_recover_saga_returns_early_for_completed_saga(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga returns early if saga is already COMPLETED."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.COMPLETED)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Recover should return early without executing steps
    await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    assert not reserve_step.act_called


async def test_recover_saga_returns_early_for_failed_saga(
    saga_container: SagaContainer,
) -> None:
    """
    Test that recover_saga handles FAILED status correctly.

    When saga is in FAILED status with no completed steps to compensate,
    recover_saga will attempt to complete compensation (which does nothing)
    and raise RuntimeError to signal that forward execution is not allowed.
    """
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.FAILED)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Recover should attempt compensation and raise RuntimeError
    # since there are no completed steps, compensation does nothing
    # but we still signal that forward execution is not allowed
    with pytest.raises(RuntimeError, match="recovered in failed state"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # No forward execution should occur
    assert not reserve_step.act_called
    # No compensation should occur (no completed steps to compensate)
    assert not reserve_step.compensate_called


async def test_recover_saga_with_class_based_context_builder(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga works with class-based context_builder."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Use class as context_builder
    await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    assert reserve_step.act_called


async def test_recover_saga_with_function_based_context_builder(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga works with function-based context_builder."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Use function as context_builder
    def context_builder(data: dict[str, typing.Any]) -> OrderContext:
        return OrderContext(**data)

    await recover_saga(saga, saga_id, context_builder, saga_container, storage)

    assert reserve_step.act_called


async def test_recover_saga_with_from_dict_method(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga works with from_dict method."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Use from_dict method as context_builder
    await recover_saga(saga, saga_id, OrderContext.from_dict, saga_container, storage)

    assert reserve_step.act_called


async def test_recover_saga_raises_on_storage_load_failure(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga raises exception when storage.load_saga_state fails."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    # Don't create saga, so load will fail
    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Should raise ValueError (from MemorySagaStorage.load_saga_state)
    with pytest.raises(ValueError, match=f"Saga {saga_id} not found"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    assert not reserve_step.act_called


async def test_recover_saga_raises_on_context_reconstruction_failure(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga raises exception when context reconstruction fails."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"invalid": "data"},  # Missing required fields
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Should raise TypeError when required fields are missing
    with pytest.raises(TypeError):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    assert not reserve_step.act_called


async def test_recover_saga_resumes_from_partially_completed_steps(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga skips already completed steps."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    # Mark first two steps as completed
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )
    await storage.log_step(
        saga_id,
        "ProcessPaymentStep",
        "act",
        SagaStepStatus.COMPLETED,
    )

    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()
    ship_step = ShipOrderStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)
    saga_container.register(ShipOrderStep, ship_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    saga = TestSaga()

    await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # First two steps should be skipped
    assert not reserve_step.act_called
    assert not payment_step.act_called

    # Last step should be executed
    assert ship_step.act_called

    # Verify saga is completed
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.COMPLETED


async def test_recover_saga_with_compensating_status(
    saga_container: SagaContainer,
) -> None:
    """
    Test that recover_saga immediately resumes compensation when saga is in COMPENSATING status.

    This test verifies the "Strict Backward Recovery" strategy: once a saga enters
    COMPENSATING status, forward execution is permanently disabled. Only compensation
    can proceed to prevent inconsistent states.
    """
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.COMPENSATING)

    # Mark first step as completed (it needs to be compensated)
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )

    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep]

    saga = TestSaga()

    # Recovery should immediately resume compensation, NOT continue forward execution
    # This is the "Point of No Return" - once COMPENSATING, we must finish compensation
    with pytest.raises(RuntimeError, match="recovered in compensating state"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # No forward execution should occur
    assert not reserve_step.act_called
    assert not payment_step.act_called

    # Compensation should have been called
    assert reserve_step.compensate_called

    # Verify saga status is FAILED (compensation completed)
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED


async def test_recover_saga_handles_transaction_exception(
    saga_container: SagaContainer,
) -> None:
    """
    Test that recover_saga propagates exceptions during transaction execution.

    When a step fails during recovery, the exception is propagated to the caller.
    The transaction handles compensation internally and updates saga status to FAILED.
    """
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    failing_step = FailingStep()
    saga_container.register(FailingStep, failing_step)

    class TestSaga(Saga[OrderContext]):
        steps = [FailingStep]

    saga = TestSaga()

    # Recovery should propagate the exception from failing step
    # The transaction handles compensation and updates saga status to FAILED
    with pytest.raises(ValueError, match="Step failed for order 123"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # Verify saga status is updated to FAILED
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED

    # Verify the step was attempted
    assert failing_step.act_called


async def test_recover_saga_with_custom_context_builder_function(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga works with custom lambda context_builder."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Use function as context_builder
    def context_builder(d: dict[str, typing.Any]) -> OrderContext:
        return OrderContext(**d)

    await recover_saga(saga, saga_id, context_builder, saga_container, storage)

    assert reserve_step.act_called


async def test_recover_saga_updates_context_during_recovery(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga properly updates context during recovery."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    initial_context = {"order_id": "123", "user_id": "user1", "amount": 100.0}
    await storage.create_saga(saga_id, "TestSaga", initial_context)
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # Verify context was updated in storage
    status, updated_context = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.COMPLETED
    assert updated_context["order_id"] == "123"


async def test_recover_saga_with_mock_storage_exception(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga properly handles storage exceptions."""
    saga_id = uuid.uuid4()

    # Create a mock storage that raises an exception
    mock_storage = AsyncMock()
    mock_storage.load_saga_state.side_effect = RuntimeError("Storage error")

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Should raise the exception
    with pytest.raises(RuntimeError, match="Storage error"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, mock_storage)

    assert not reserve_step.act_called


async def test_recover_saga_with_mock_context_builder_exception(
    saga_container: SagaContainer,
) -> None:
    """Test that recover_saga properly handles context builder exceptions."""
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.RUNNING)

    reserve_step = ReserveInventoryStep()
    saga_container.register(ReserveInventoryStep, reserve_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = TestSaga()

    # Context builder that raises an exception
    def failing_context_builder(data: dict[str, typing.Any]) -> OrderContext:
        raise ValueError("Context builder failed")

    # Should raise the exception
    with pytest.raises(ValueError, match="Context builder failed"):
        await recover_saga(
            saga,
            saga_id,
            failing_context_builder,
            saga_container,
            storage,
        )

    assert not reserve_step.act_called


async def test_recover_saga_during_compensation_with_multiple_steps(
    saga_container: SagaContainer,
) -> None:
    """
    Test recovery during compensation with multiple completed steps.

    Scenario:
    - Steps A and B completed successfully
    - Step C failed, saga entered COMPENSATING
    - Step C compensation completed
    - System crashed during compensation of step B
    - On recovery: should complete compensation of B and A, NOT retry step C
    """
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.COMPENSATING)

    # Mark steps A and B as completed (need compensation)
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )
    await storage.log_step(
        saga_id,
        "ProcessPaymentStep",
        "act",
        SagaStepStatus.COMPLETED,
    )

    # Mark step C as failed (no compensation needed, it never succeeded)
    await storage.log_step(
        saga_id,
        "ShipOrderStep",
        "act",
        SagaStepStatus.FAILED,
        "Network error",
    )

    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()
    ship_step = ShipOrderStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)
    saga_container.register(ShipOrderStep, ship_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    saga = TestSaga()

    # Recovery should complete compensation, NOT retry step C
    with pytest.raises(RuntimeError, match="recovered in compensating state"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # No forward execution should occur
    assert not reserve_step.act_called
    assert not payment_step.act_called
    assert not ship_step.act_called

    # Compensation should have been called for completed steps (in reverse order)
    assert payment_step.compensate_called
    assert reserve_step.compensate_called

    # Verify saga status is FAILED (compensation completed)
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED


async def test_recover_saga_with_failed_status(
    saga_container: SagaContainer,
) -> None:
    """
    Test recovery when saga is in FAILED status.

    FAILED status means compensation was attempted but may have failed.
    On recovery, we should NOT attempt forward execution, only complete
    any remaining compensation.
    """
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.FAILED)

    # Mark step as completed (may need compensation if it wasn't completed)
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )

    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep]

    saga = TestSaga()

    # Recovery should immediately resume compensation, NOT continue forward execution
    with pytest.raises(RuntimeError, match="recovered in failed state"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # No forward execution should occur
    assert not reserve_step.act_called
    assert not payment_step.act_called

    # Compensation should have been called
    assert reserve_step.compensate_called

    # Verify saga status remains FAILED
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED


async def test_recover_saga_prevents_zombie_state(
    saga_container: SagaContainer,
) -> None:
    """
    Test that recovery prevents "zombie state" where partial compensation
    conflicts with new execution attempts.

    Scenario:
    - Steps A and B completed
    - Step C failed, saga entered COMPENSATING
    - Compensation C completed (nothing to undo)
    - System crashed during compensation B
    - On recovery: if we allowed forward execution, step C might succeed
      (network fixed), but we already started refunding (compensation B)
    - Result: client gets free product (refunded + shipped)

    With Strict Backward Recovery: we MUST complete compensation only.
    """
    storage = MemorySagaStorage()
    saga_id = uuid.uuid4()

    await storage.create_saga(
        saga_id,
        "TestSaga",
        {"order_id": "123", "user_id": "user1", "amount": 100.0},
    )
    await storage.update_status(saga_id, SagaStatus.COMPENSATING)

    # Mark steps A and B as completed
    await storage.log_step(
        saga_id,
        "ReserveInventoryStep",
        "act",
        SagaStepStatus.COMPLETED,
    )
    await storage.log_step(
        saga_id,
        "ProcessPaymentStep",
        "act",
        SagaStepStatus.COMPLETED,
    )

    # Mark step C as failed (never completed)
    await storage.log_step(
        saga_id,
        "ShipOrderStep",
        "act",
        SagaStepStatus.FAILED,
        "Network timeout",
    )

    # Mark that compensation C was attempted (nothing to undo, so it "succeeded")
    # This simulates: we tried to compensate C, but there was nothing to undo
    await storage.log_step(
        saga_id,
        "ShipOrderStep",
        "compensate",
        SagaStepStatus.COMPLETED,
    )

    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()
    ship_step = ShipOrderStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)
    saga_container.register(ShipOrderStep, ship_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    saga = TestSaga()

    # Recovery should complete compensation, NOT retry step C
    # Even if network is now available and step C would succeed,
    # we must finish compensation to prevent inconsistent state
    with pytest.raises(RuntimeError, match="recovered in compensating state"):
        await recover_saga(saga, saga_id, OrderContext, saga_container, storage)

    # CRITICAL: Step C should NOT be executed, even though it might succeed now
    assert not ship_step.act_called, (
        "Step C should not be retried during recovery in COMPENSATING state. "
        "This would create a zombie state: partial compensation + new execution."
    )

    # Compensation should complete for steps A and B
    assert payment_step.compensate_called
    assert reserve_step.compensate_called

    # Verify saga status is FAILED
    status, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED

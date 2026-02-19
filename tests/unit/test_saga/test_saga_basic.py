"""Basic saga execution tests."""

import pytest

from cqrs.saga.saga import Saga
from cqrs.saga.storage.memory import MemorySagaStorage  # noqa: F401
from .conftest import (
    CompensationFailingStep,
    FailingStep,
    OrderContext,
    ProcessPaymentResponse,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderResponse,
    ShipOrderStep,
)


async def test_saga_executes_all_steps_successfully(
    successful_saga: Saga[OrderContext],
    saga_container: SagaContainer,
    storage: MemorySagaStorage,
) -> None:
    """Test that saga executes all steps successfully and yields results."""
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    step_results = []
    async with successful_saga.transaction(
        context=context,
        container=saga_container,  # type: ignore
        storage=storage,
    ) as transaction:
        async for step_result in transaction:
            step_results.append(step_result)

    assert len(step_results) == 3

    # Check first step
    assert isinstance(step_results[0].response, ReserveInventoryResponse)
    assert step_results[0].response.inventory_id == "inv_123"
    assert step_results[0].response.reserved is True
    assert step_results[0].step_type == ReserveInventoryStep

    # Check second step
    assert isinstance(step_results[1].response, ProcessPaymentResponse)
    assert step_results[1].response.payment_id == "pay_123"
    assert step_results[1].response.charged is True
    assert step_results[1].step_type == ProcessPaymentStep

    # Check third step
    assert isinstance(step_results[2].response, ShipOrderResponse)
    assert step_results[2].response.shipment_id == "ship_123"
    assert step_results[2].response.shipped is True
    assert step_results[2].step_type == ShipOrderStep


async def test_saga_calls_act_on_all_steps(
    saga_container: SagaContainer,
) -> None:
    """Test that saga calls act method on all step handlers."""
    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()
    ship_step = ShipOrderStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)
    saga_container.register(ShipOrderStep, ship_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    storage_instance = MemorySagaStorage()
    async with saga.transaction(
        context=context,
        container=saga_container,  # type: ignore
        storage=storage_instance,
    ) as transaction:
        async for _ in transaction:
            pass

    assert reserve_step.act_called
    assert payment_step.act_called
    assert ship_step.act_called

    assert not reserve_step.compensate_called
    assert not payment_step.compensate_called
    assert not ship_step.compensate_called


async def test_saga_compensates_on_failure(
    saga_container: SagaContainer,
) -> None:
    """Test that saga compensates all completed steps when a step fails."""
    reserve_step = ReserveInventoryStep()
    payment_step = ProcessPaymentStep()
    failing_step = FailingStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(ProcessPaymentStep, payment_step)
    saga_container.register(FailingStep, failing_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, FailingStep]

    saga = TestSaga()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError, match="Step failed for order 123"):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=MemorySagaStorage(),
        ) as transaction:
            async for _ in transaction:
                pass

    # First two steps should have been executed
    assert reserve_step.act_called
    assert payment_step.act_called
    assert failing_step.act_called

    # First two steps should have been compensated
    assert reserve_step.compensate_called
    assert payment_step.compensate_called
    # Failing step should not be compensated (it never completed successfully)
    assert not failing_step.compensate_called


async def test_saga_compensates_in_reverse_order(
    saga_container: SagaContainer,
) -> None:
    """Test that saga compensates steps in reverse order of execution."""
    compensation_order = []

    class TrackingReserveStep(ReserveInventoryStep):
        async def compensate(self, context: OrderContext) -> None:
            compensation_order.append("reserve")
            await super().compensate(context)

    class TrackingPaymentStep(ProcessPaymentStep):
        async def compensate(self, context: OrderContext) -> None:
            compensation_order.append("payment")
            await super().compensate(context)

    reserve_step = TrackingReserveStep()
    payment_step = TrackingPaymentStep()
    failing_step = FailingStep()

    saga_container.register(TrackingReserveStep, reserve_step)
    saga_container.register(TrackingPaymentStep, payment_step)
    saga_container.register(FailingStep, failing_step)

    class TestSaga(Saga[OrderContext]):
        steps = [TrackingReserveStep, TrackingPaymentStep, FailingStep]

    saga = TestSaga()
    storage_instance = MemorySagaStorage()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    with pytest.raises(ValueError):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=storage_instance,
        ) as transaction:
            async for _ in transaction:
                pass

    # Compensation should happen in reverse order
    assert compensation_order == ["payment", "reserve"]


async def test_saga_handles_compensation_failure_gracefully(
    saga_container: SagaContainer,
) -> None:
    """Test that saga handles compensation failures without masking original error."""
    reserve_step = ReserveInventoryStep()
    compensation_failing_step = CompensationFailingStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(CompensationFailingStep, compensation_failing_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, CompensationFailingStep, FailingStep]

    saga = TestSaga()
    storage_instance = MemorySagaStorage()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    # Original error should still be raised even if compensation fails
    with pytest.raises(ValueError, match="Step failed for order 123"):
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=storage_instance,
        ) as transaction:
            async for _ in transaction:
                pass

    assert reserve_step.act_called
    assert compensation_failing_step.act_called
    assert reserve_step.compensate_called
    assert compensation_failing_step.compensate_called


async def test_saga_transaction_context_manager_exits_correctly(
    successful_saga: Saga[OrderContext],
    saga_container: SagaContainer,
    storage: MemorySagaStorage,
) -> None:
    """Test that transaction context manager exits correctly on success."""
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)
    step_count = 0

    async with successful_saga.transaction(
        context=context,
        container=saga_container,  # type: ignore
        storage=storage,
    ) as transaction:
        async for step_result in transaction:
            step_count += 1
            assert step_result is not None

    assert step_count == 3


async def test_saga_transaction_context_manager_exits_on_exception(
    saga_container: SagaContainer,
) -> None:
    """Test that transaction context manager exits correctly on exception."""
    reserve_step = ReserveInventoryStep()
    failing_step = FailingStep()

    saga_container.register(ReserveInventoryStep, reserve_step)
    saga_container.register(FailingStep, failing_step)

    class TestSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, FailingStep]

    saga = TestSaga()
    storage_instance = MemorySagaStorage()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    try:
        async with saga.transaction(
            context=context,
            container=saga_container,  # type: ignore
            storage=storage_instance,
        ) as transaction:
            async for _ in transaction:
                pass
    except ValueError:
        pass  # Expected exception

    assert reserve_step.compensate_called


async def test_saga_with_empty_steps_list(saga_container: SagaContainer) -> None:
    """Test that saga handles empty steps list correctly."""

    class EmptySaga(Saga[OrderContext]):
        steps = []

    saga = EmptySaga()
    storage_instance = MemorySagaStorage()

    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    step_results = []
    async with saga.transaction(
        context=context,
        container=saga_container,  # type: ignore
        storage=storage_instance,
    ) as transaction:
        async for step_result in transaction:
            step_results.append(step_result)

    assert len(step_results) == 0


async def test_saga_step_result_contains_correct_metadata(
    successful_saga: Saga[OrderContext],
    saga_container: SagaContainer,
    storage: MemorySagaStorage,
) -> None:
    """Test that SagaStepResult contains correct metadata."""
    context = OrderContext(order_id="123", user_id="user1", amount=100.0)

    step_results = []
    async with successful_saga.transaction(
        context=context,
        container=saga_container,  # type: ignore
        storage=storage,
    ) as transaction:
        async for step_result in transaction:
            step_results.append(step_result)

    # Check that all results have correct structure
    for step_result in step_results:
        assert step_result.response is not None
        assert step_result.step_type is not None
        assert step_result.with_error is False
        assert step_result.error_message is None
        assert step_result.error_traceback is None
        assert step_result.error_type is None
        assert step_result.saga_id is not None

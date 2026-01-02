"""Integration tests for MemorySagaStorage."""

import uuid

import pytest

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage


@pytest.fixture
def storage() -> MemorySagaStorage:
    """Create a fresh MemorySagaStorage instance for each test."""
    return MemorySagaStorage()


@pytest.fixture
def saga_id() -> uuid.UUID:
    """Generate a test saga ID."""
    return uuid.uuid4()


@pytest.fixture
def test_context() -> dict[str, str]:
    """Test context data."""
    return {"order_id": "123", "user_id": "user1", "amount": "100.0"}


class TestIntegration:
    """Integration tests for multiple operations."""

    async def test_full_saga_lifecycle(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test complete saga lifecycle with all operations."""
        # Create saga
        await storage.create_saga(
            saga_id=saga_id,
            name="order_saga",
            context=test_context,
        )

        # Update status to running
        await storage.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)

        # Log step executions
        await storage.log_step(
            saga_id=saga_id,
            step_name="reserve_inventory",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await storage.log_step(
            saga_id=saga_id,
            step_name="reserve_inventory",
            action="act",
            status=SagaStepStatus.COMPLETED,
        )

        await storage.log_step(
            saga_id=saga_id,
            step_name="process_payment",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await storage.log_step(
            saga_id=saga_id,
            step_name="process_payment",
            action="act",
            status=SagaStepStatus.COMPLETED,
        )

        # Update context
        updated_context = {**test_context, "payment_id": "pay_123"}
        await storage.update_context(saga_id=saga_id, context=updated_context)

        # Update status to completed
        await storage.update_status(saga_id=saga_id, status=SagaStatus.COMPLETED)

        # Verify final state
        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPLETED
        assert context == updated_context

        # Verify history
        history = await storage.get_step_history(saga_id)
        assert len(history) == 4
        assert history[0].step_name == "reserve_inventory"
        assert history[0].status == SagaStepStatus.STARTED
        assert history[1].step_name == "reserve_inventory"
        assert history[1].status == SagaStepStatus.COMPLETED
        assert history[2].step_name == "process_payment"
        assert history[2].status == SagaStepStatus.STARTED
        assert history[3].step_name == "process_payment"
        assert history[3].status == SagaStepStatus.COMPLETED

    async def test_compensation_scenario(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test saga compensation scenario."""
        await storage.create_saga(
            saga_id=saga_id,
            name="order_saga",
            context=test_context,
        )

        # Log successful steps
        await storage.log_step(
            saga_id=saga_id,
            step_name="reserve_inventory",
            action="act",
            status=SagaStepStatus.COMPLETED,
        )
        await storage.log_step(
            saga_id=saga_id,
            step_name="process_payment",
            action="act",
            status=SagaStepStatus.COMPLETED,
        )

        # Update status to compensating
        await storage.update_status(saga_id=saga_id, status=SagaStatus.COMPENSATING)

        # Log compensation steps
        await storage.log_step(
            saga_id=saga_id,
            step_name="process_payment",
            action="compensate",
            status=SagaStepStatus.COMPENSATED,
            details="Payment refunded",
        )
        await storage.log_step(
            saga_id=saga_id,
            step_name="reserve_inventory",
            action="compensate",
            status=SagaStepStatus.COMPENSATED,
            details="Inventory released",
        )

        # Update status to failed
        await storage.update_status(saga_id=saga_id, status=SagaStatus.FAILED)

        # Verify state
        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED

        # Verify history
        history = await storage.get_step_history(saga_id)
        assert len(history) == 4
        assert history[0].action == "act"
        assert history[1].action == "act"
        assert history[2].action == "compensate"
        assert history[3].action == "compensate"
        assert history[2].details == "Payment refunded"
        assert history[3].details == "Inventory released"

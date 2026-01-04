"""Integration tests for SqlAlchemySagaStorage."""

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

# Fixtures init_saga_orm and saga_session_factory are imported from tests/integration/fixtures.py


@pytest.fixture
def storage(
    saga_session_factory: async_sessionmaker[AsyncSession],
) -> SqlAlchemySagaStorage:
    """Create a SqlAlchemySagaStorage instance for each test."""
    return SqlAlchemySagaStorage(saga_session_factory)


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
        storage: SqlAlchemySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test complete saga lifecycle with all operations."""
        # Create saga (storage handles transaction commit internally)
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
        storage: SqlAlchemySagaStorage,
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

    async def test_persistence_across_sessions(
        self,
        saga_session_factory: async_sessionmaker[AsyncSession],
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that saga state persists across different storage instances."""
        # Create saga with first storage instance
        storage1 = SqlAlchemySagaStorage(saga_session_factory)
        await storage1.create_saga(
            saga_id=saga_id,
            name="order_saga",
            context=test_context,
        )
        await storage1.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)
        await storage1.log_step(
            saga_id=saga_id,
            step_name="step1",
            action="act",
            status=SagaStepStatus.COMPLETED,
        )

        # Create new storage instance and verify persistence
        # Note: Since storage now commits internally, data is already persisted
        storage2 = SqlAlchemySagaStorage(saga_session_factory)
        status, context = await storage2.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING
        assert context == test_context

        history = await storage2.get_step_history(saga_id)
        assert len(history) == 1
        assert history[0].step_name == "step1"
        assert history[0].status == SagaStepStatus.COMPLETED

    async def test_concurrent_updates(
        self,
        storage: SqlAlchemySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test handling of multiple sequential updates."""
        await storage.create_saga(
            saga_id=saga_id,
            name="order_saga",
            context=test_context,
        )

        # Perform multiple updates
        await storage.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)
        await storage.update_context(saga_id=saga_id, context={"updated": "context1"})
        await storage.update_status(saga_id=saga_id, status=SagaStatus.COMPENSATING)
        await storage.update_context(saga_id=saga_id, context={"updated": "context2"})

        # Verify final state
        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPENSATING
        assert context == {"updated": "context2"}

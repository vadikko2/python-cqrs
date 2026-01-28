"""Integration tests for SqlAlchemySagaStorage."""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.sqlalchemy import (
    SagaExecutionModel,
    SagaLogModel,
    SqlAlchemySagaStorage,
)

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
        status, context, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPLETED
        assert context == updated_context
        # Initial create(1) + update_status(RUNNING)(2) + update_context(3) + update_status(COMPLETED)(4) = 4
        assert version == 4

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
        status, context, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED
        # Initial create(1) + update_status(COMPENSATING)(2) + update_status(FAILED)(3) = 3
        assert version == 3

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
        status, context, version = await storage2.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING
        assert context == test_context
        assert version == 2  # create + update_status

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
        status, context, version = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPENSATING
        assert context == {"updated": "context2"}
        assert version == 5

    async def test_optimistic_locking(
        self,
        storage: SqlAlchemySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that optimistic locking prevents concurrent modifications."""
        await storage.create_saga(
            saga_id=saga_id,
            name="order_saga",
            context=test_context,
        )

        # Get initial state
        _, _, version = await storage.load_saga_state(saga_id)
        assert version == 1

        # Successful update with correct version
        new_context = {**test_context, "updated": True}
        await storage.update_context(saga_id, new_context, current_version=version)

        # Verify version incremented
        _, _, new_version = await storage.load_saga_state(saga_id)
        assert new_version == 2

        # Failed update with old version
        with pytest.raises(SagaConcurrencyError):
            await storage.update_context(
                saga_id,
                {"stale": True},
                current_version=version,  # Using old version 1
            )

        # State should not have changed
        _, final_context, final_version = await storage.load_saga_state(saga_id)
        assert final_context == new_context
        assert final_version == 2


class TestRecoverySqlAlchemy:
    """Integration tests for get_sagas_for_recovery and increment_recovery_attempts (SqlAlchemy)."""

    @pytest.fixture(autouse=True)
    async def _clean_saga_tables(
        self,
        saga_session_factory: async_sessionmaker[AsyncSession],
    ) -> AsyncGenerator[None, None]:
        """Clear saga tables before each test so get_sagas_for_recovery sees only this test's data."""
        async with saga_session_factory() as session:
            await session.execute(delete(SagaLogModel))
            await session.execute(delete(SagaExecutionModel))
            await session.commit()
        yield

    # --- get_sagas_for_recovery: positive ---

    async def test_get_sagas_for_recovery_returns_recoverable_sagas(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: returns RUNNING and COMPENSATING sagas only; FAILED excluded."""
        id1, id2, id3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        for sid in (id1, id2, id3):
            await storage.create_saga(saga_id=sid, name="saga", context=test_context)
        await storage.update_status(id1, SagaStatus.RUNNING)
        await storage.update_status(id2, SagaStatus.COMPENSATING)
        await storage.update_status(id3, SagaStatus.FAILED)

        ids = await storage.get_sagas_for_recovery(limit=10)
        assert set(ids) == {id1, id2}
        assert id3 not in ids
        assert len(ids) == 2

    async def test_get_sagas_for_recovery_respects_limit(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: returns at most `limit` saga IDs."""
        for _ in range(5):
            sid = uuid.uuid4()
            await storage.create_saga(saga_id=sid, name="saga", context=test_context)
            await storage.update_status(sid, SagaStatus.RUNNING)

        ids = await storage.get_sagas_for_recovery(limit=2)
        assert len(ids) == 2

    async def test_get_sagas_for_recovery_respects_max_recovery_attempts(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: only returns sagas with recovery_attempts < max_recovery_attempts."""
        id_low = uuid.uuid4()
        id_high = uuid.uuid4()
        await storage.create_saga(saga_id=id_low, name="saga", context=test_context)
        await storage.create_saga(saga_id=id_high, name="saga", context=test_context)
        await storage.update_status(id_low, SagaStatus.RUNNING)
        await storage.update_status(id_high, SagaStatus.RUNNING)
        for _ in range(5):
            await storage.increment_recovery_attempts(id_high)

        ids = await storage.get_sagas_for_recovery(limit=10, max_recovery_attempts=5)
        assert id_low in ids
        assert id_high not in ids

    async def test_get_sagas_for_recovery_ordered_by_updated_at(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: result ordered by updated_at ascending (oldest first)."""
        id1, id2, id3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        for sid in (id1, id2, id3):
            await storage.create_saga(saga_id=sid, name="saga", context=test_context)
            await storage.update_status(sid, SagaStatus.RUNNING)
        # Ensure id2 has a strictly later updated_at (DB may use second precision).
        await asyncio.sleep(1.0)
        await storage.update_context(id2, {**test_context, "touched": True})

        ids = await storage.get_sagas_for_recovery(limit=10)
        assert len(ids) == 3
        assert ids[-1] == id2

    async def test_get_sagas_for_recovery_stale_after_excludes_recently_updated(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: with stale_after_seconds, recently updated sagas are excluded."""
        id_recent = uuid.uuid4()
        await storage.create_saga(saga_id=id_recent, name="saga", context=test_context)
        await storage.update_status(id_recent, SagaStatus.RUNNING)
        ids = await storage.get_sagas_for_recovery(
            limit=10,
            stale_after_seconds=999999,
        )
        assert id_recent not in ids

    async def test_get_sagas_for_recovery_without_stale_after_unchanged_behavior(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Backward compat: without stale_after_seconds, recently updated sagas are included."""
        sid = uuid.uuid4()
        await storage.create_saga(saga_id=sid, name="saga", context=test_context)
        await storage.update_status(sid, SagaStatus.RUNNING)
        ids = await storage.get_sagas_for_recovery(limit=10)
        assert sid in ids

    async def test_get_sagas_for_recovery_filters_by_saga_name_when_provided(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: when saga_name is set, only sagas with that name are returned."""
        id_foo1 = uuid.uuid4()
        id_foo2 = uuid.uuid4()
        id_bar = uuid.uuid4()
        await storage.create_saga(
            saga_id=id_foo1,
            name="OrderSaga",
            context=test_context,
        )
        await storage.create_saga(
            saga_id=id_foo2,
            name="OrderSaga",
            context=test_context,
        )
        await storage.create_saga(
            saga_id=id_bar,
            name="PaymentSaga",
            context=test_context,
        )
        await storage.update_status(id_foo1, SagaStatus.RUNNING)
        await storage.update_status(id_foo2, SagaStatus.RUNNING)
        await storage.update_status(id_bar, SagaStatus.RUNNING)

        ids_all = await storage.get_sagas_for_recovery(limit=10)
        assert len(ids_all) == 3
        ids_order = await storage.get_sagas_for_recovery(
            limit=10,
            saga_name="OrderSaga",
        )
        assert set(ids_order) == {id_foo1, id_foo2}
        ids_payment = await storage.get_sagas_for_recovery(
            limit=10,
            saga_name="PaymentSaga",
        )
        assert ids_payment == [id_bar]
        ids_nonexistent = await storage.get_sagas_for_recovery(
            limit=10,
            saga_name="NonExistentSaga",
        )
        assert ids_nonexistent == []

    async def test_get_sagas_for_recovery_saga_name_none_returns_all_types(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Backward compat: when saga_name is None, all saga types are returned."""
        id1 = uuid.uuid4()
        id2 = uuid.uuid4()
        await storage.create_saga(saga_id=id1, name="SagaA", context=test_context)
        await storage.create_saga(saga_id=id2, name="SagaB", context=test_context)
        await storage.update_status(id1, SagaStatus.RUNNING)
        await storage.update_status(id2, SagaStatus.RUNNING)
        ids = await storage.get_sagas_for_recovery(limit=10, saga_name=None)
        assert set(ids) == {id1, id2}

    # --- get_sagas_for_recovery: negative ---

    async def test_get_sagas_for_recovery_empty_when_none_recoverable(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Negative: returns empty list when no recoverable sagas."""
        sid = uuid.uuid4()
        await storage.create_saga(saga_id=sid, name="saga", context=test_context)
        await storage.update_status(sid, SagaStatus.COMPLETED)

        ids = await storage.get_sagas_for_recovery(limit=10)
        assert ids == []

    async def test_get_sagas_for_recovery_excludes_pending_and_completed(
        self,
        storage: SqlAlchemySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Negative: PENDING and COMPLETED sagas are not returned."""
        id_pending = uuid.uuid4()
        id_completed = uuid.uuid4()
        await storage.create_saga(saga_id=id_pending, name="saga", context=test_context)
        await storage.create_saga(
            saga_id=id_completed,
            name="saga",
            context=test_context,
        )
        await storage.update_status(id_completed, SagaStatus.COMPLETED)

        ids = await storage.get_sagas_for_recovery(limit=10)
        assert id_pending not in ids
        assert id_completed not in ids

    # --- increment_recovery_attempts: positive ---

    async def test_increment_recovery_attempts_increments_counter(
        self,
        storage: SqlAlchemySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Positive: recovery_attempts increases; saga drops out after max_recovery_attempts."""
        await storage.create_saga(saga_id=saga_id, name="saga", context=test_context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)

        ids_before = await storage.get_sagas_for_recovery(
            limit=10,
            max_recovery_attempts=5,
        )
        assert saga_id in ids_before

        for _ in range(5):
            await storage.increment_recovery_attempts(saga_id)

        ids_after = await storage.get_sagas_for_recovery(
            limit=10,
            max_recovery_attempts=5,
        )
        assert saga_id not in ids_after

    async def test_increment_recovery_attempts_with_new_status(
        self,
        storage: SqlAlchemySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Positive: optional new_status updates saga status."""
        await storage.create_saga(saga_id=saga_id, name="saga", context=test_context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)

        await storage.increment_recovery_attempts(saga_id, new_status=SagaStatus.FAILED)
        status, _, _ = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED

    # --- increment_recovery_attempts: negative ---

    async def test_increment_recovery_attempts_raises_when_saga_not_found(
        self,
        storage: SqlAlchemySagaStorage,
    ) -> None:
        """Negative: raises ValueError when saga_id does not exist."""
        unknown_id = uuid.uuid4()
        with pytest.raises(ValueError, match="not found"):
            await storage.increment_recovery_attempts(unknown_id)

    # --- set_recovery_attempts: positive ---

    async def test_set_recovery_attempts_sets_value(
        self,
        storage: SqlAlchemySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Positive: recovery_attempts is set to the given value."""
        await storage.create_saga(saga_id=saga_id, name="saga", context=test_context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)
        await storage.increment_recovery_attempts(saga_id)
        await storage.increment_recovery_attempts(saga_id)

        await storage.set_recovery_attempts(saga_id, 0)
        ids_after_reset = await storage.get_sagas_for_recovery(
            limit=10,
            max_recovery_attempts=5,
        )
        assert saga_id in ids_after_reset

        await storage.set_recovery_attempts(saga_id, 5)
        ids_after_max = await storage.get_sagas_for_recovery(
            limit=10,
            max_recovery_attempts=5,
        )
        assert saga_id not in ids_after_max

    async def test_set_recovery_attempts_raises_when_saga_not_found(
        self,
        storage: SqlAlchemySagaStorage,
    ) -> None:
        """Negative: raises ValueError when saga_id does not exist."""
        unknown_id = uuid.uuid4()
        with pytest.raises(ValueError, match="not found"):
            await storage.set_recovery_attempts(unknown_id, 0)

"""Integration tests for MemorySagaStorage."""

import datetime
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
        status, context, version = await storage.load_saga_state(saga_id)
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
        status, context, version = await storage.load_saga_state(saga_id)
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


class TestRecoveryMemory:
    """Integration tests for get_sagas_for_recovery and increment_recovery_attempts (Memory)."""

    # --- get_sagas_for_recovery: positive ---

    async def test_get_sagas_for_recovery_returns_recoverable_sagas(
        self,
        storage: MemorySagaStorage,
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
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: returns at most `limit` saga IDs."""
        for i in range(5):
            sid = uuid.uuid4()
            await storage.create_saga(saga_id=sid, name="saga", context=test_context)
            await storage.update_status(sid, SagaStatus.RUNNING)

        ids = await storage.get_sagas_for_recovery(limit=2)
        assert len(ids) == 2

    async def test_get_sagas_for_recovery_respects_max_recovery_attempts(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: only returns sagas with recovery_attempts < max_recovery_attempts."""
        id_low = uuid.uuid4()
        id_high = uuid.uuid4()
        await storage.create_saga(saga_id=id_low, name="saga", context=test_context)
        await storage.create_saga(saga_id=id_high, name="saga", context=test_context)
        await storage.update_status(id_low, SagaStatus.RUNNING)
        await storage.update_status(id_high, SagaStatus.RUNNING)
        # id_high: simulate 5 failed recovery attempts (default max is 5)
        for _ in range(5):
            await storage.increment_recovery_attempts(id_high)

        ids = await storage.get_sagas_for_recovery(limit=10, max_recovery_attempts=5)
        assert id_low in ids
        assert id_high not in ids

    async def test_get_sagas_for_recovery_ordered_by_updated_at(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: result ordered by updated_at ascending (oldest first)."""
        id1, id2, id3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        for sid in (id1, id2, id3):
            await storage.create_saga(saga_id=sid, name="saga", context=test_context)
            await storage.update_status(sid, SagaStatus.RUNNING)
        # touch id2 so its updated_at is latest
        await storage.update_context(id2, {**test_context, "touched": True})

        ids = await storage.get_sagas_for_recovery(limit=10)
        assert len(ids) == 3
        # id2 was updated last, so should be last in list (oldest first)
        assert ids[-1] == id2

    async def test_get_sagas_for_recovery_stale_after_excludes_recently_updated(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: with stale_after_seconds, recently updated sagas are excluded."""
        id_recent = uuid.uuid4()
        await storage.create_saga(saga_id=id_recent, name="saga", context=test_context)
        await storage.update_status(id_recent, SagaStatus.RUNNING)
        # No manual change to updated_at: it was just updated
        ids = await storage.get_sagas_for_recovery(
            limit=10,
            stale_after_seconds=60,
        )
        assert id_recent not in ids

    async def test_get_sagas_for_recovery_stale_after_includes_old_updated(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Positive: with stale_after_seconds, sagas with old updated_at are included."""
        id_old = uuid.uuid4()
        await storage.create_saga(saga_id=id_old, name="saga", context=test_context)
        await storage.update_status(id_old, SagaStatus.RUNNING)
        storage._sagas[id_old]["updated_at"] = datetime.datetime.now(
            datetime.timezone.utc,
        ) - datetime.timedelta(seconds=120)
        ids = await storage.get_sagas_for_recovery(
            limit=10,
            stale_after_seconds=60,
        )
        assert id_old in ids

    async def test_get_sagas_for_recovery_without_stale_after_unchanged_behavior(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Backward compat: without stale_after_seconds, recently updated sagas are included."""
        sid = uuid.uuid4()
        await storage.create_saga(saga_id=sid, name="saga", context=test_context)
        await storage.update_status(sid, SagaStatus.RUNNING)
        ids = await storage.get_sagas_for_recovery(limit=10)
        assert sid in ids

    # --- get_sagas_for_recovery: negative ---

    async def test_get_sagas_for_recovery_empty_when_none_recoverable(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Negative: returns empty list when no recoverable sagas."""
        sid = uuid.uuid4()
        await storage.create_saga(saga_id=sid, name="saga", context=test_context)
        # PENDING and COMPLETED are not recoverable
        await storage.update_status(sid, SagaStatus.COMPLETED)

        ids = await storage.get_sagas_for_recovery(limit=10)
        assert ids == []

    async def test_get_sagas_for_recovery_excludes_pending_and_completed(
        self,
        storage: MemorySagaStorage,
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
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Positive: recovery_attempts increases by 1 each call."""
        await storage.create_saga(saga_id=saga_id, name="saga", context=test_context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)

        await storage.increment_recovery_attempts(saga_id)
        _, ctx, ver = await storage.load_saga_state(saga_id)
        assert storage._sagas[saga_id]["recovery_attempts"] == 1

        await storage.increment_recovery_attempts(saga_id)
        assert storage._sagas[saga_id]["recovery_attempts"] == 2

    async def test_increment_recovery_attempts_updates_updated_at(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Positive: updated_at is set to now."""
        await storage.create_saga(saga_id=saga_id, name="saga", context=test_context)
        await storage.update_status(saga_id, SagaStatus.RUNNING)
        before = storage._sagas[saga_id]["updated_at"]

        await storage.increment_recovery_attempts(saga_id)
        after = storage._sagas[saga_id]["updated_at"]
        assert after >= before

    async def test_increment_recovery_attempts_with_new_status(
        self,
        storage: MemorySagaStorage,
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
        storage: MemorySagaStorage,
    ) -> None:
        """Negative: raises ValueError when saga_id does not exist."""
        unknown_id = uuid.uuid4()
        with pytest.raises(ValueError, match="not found"):
            await storage.increment_recovery_attempts(unknown_id)

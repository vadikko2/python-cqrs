"""Tests for MemorySagaStorage implementation."""

import asyncio
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


class TestCreateSaga:
    """Tests for create_saga method."""

    async def test_create_saga_success(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test successful saga creation."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.PENDING
        assert context == test_context

    async def test_create_saga_initializes_empty_logs(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that saga creation initializes empty log list."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        history = await storage.get_step_history(saga_id)
        assert history == []

    async def test_create_saga_duplicate_id_raises_error(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that creating saga with duplicate ID raises ValueError."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        with pytest.raises(ValueError, match=f"Saga {saga_id} already exists"):
            await storage.create_saga(
                saga_id=saga_id,
                name="another_saga",
                context={"different": "context"},
            )

    async def test_create_saga_sets_timestamps(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that saga creation sets created_at and updated_at timestamps."""
        before = datetime.datetime.now(datetime.timezone.utc)
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )
        after = datetime.datetime.now(datetime.timezone.utc)

        # Access internal state to check timestamps
        saga_data = storage._sagas[saga_id]
        assert before <= saga_data["created_at"] <= after
        assert before <= saga_data["updated_at"] <= after
        assert saga_data["created_at"] == saga_data["updated_at"]


class TestUpdateContext:
    """Tests for update_context method."""

    async def test_update_context_success(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test successful context update."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        new_context = {"order_id": "456", "user_id": "user2", "amount": "200.0"}
        await storage.update_context(saga_id=saga_id, context=new_context)

        status, context = await storage.load_saga_state(saga_id)
        assert context == new_context
        assert status == SagaStatus.PENDING  # Status should remain unchanged

    async def test_update_context_updates_timestamp(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that context update updates updated_at timestamp."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        original_updated_at = storage._sagas[saga_id]["updated_at"]
        # Small delay to ensure timestamp difference
        await asyncio.sleep(0.01)

        new_context = {"updated": "context"}
        await storage.update_context(saga_id=saga_id, context=new_context)

        updated_at = storage._sagas[saga_id]["updated_at"]
        assert updated_at > original_updated_at

    async def test_update_context_nonexistent_saga_raises_error(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
    ) -> None:
        """Test that updating context for nonexistent saga raises ValueError."""
        with pytest.raises(ValueError, match=f"Saga {saga_id} not found"):
            await storage.update_context(
                saga_id=saga_id,
                context={"some": "context"},
            )

    async def test_update_context_replaces_entire_context(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that update_context replaces entire context, not merges."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        partial_context = {"new_field": "new_value"}
        await storage.update_context(saga_id=saga_id, context=partial_context)

        _, context = await storage.load_saga_state(saga_id)
        assert context == partial_context
        assert "order_id" not in context
        assert "user_id" not in context


class TestUpdateStatus:
    """Tests for update_status method."""

    async def test_update_status_success(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test successful status update."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        await storage.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)
        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING
        assert context == test_context  # Context should remain unchanged

    async def test_update_status_all_statuses(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test updating status to all possible values."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        for saga_status in SagaStatus:
            await storage.update_status(saga_id=saga_id, status=saga_status)
            status, _ = await storage.load_saga_state(saga_id)
            assert status == saga_status

    async def test_update_status_updates_timestamp(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that status update updates updated_at timestamp."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        original_updated_at = storage._sagas[saga_id]["updated_at"]
        await asyncio.sleep(0.01)

        await storage.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)

        updated_at = storage._sagas[saga_id]["updated_at"]
        assert updated_at > original_updated_at

    async def test_update_status_nonexistent_saga_raises_error(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
    ) -> None:
        """Test that updating status for nonexistent saga raises ValueError."""
        with pytest.raises(ValueError, match=f"Saga {saga_id} not found"):
            await storage.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)


class TestLogStep:
    """Tests for log_step method."""

    async def test_log_step_act_started(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test logging act step with STARTED status."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        await storage.log_step(
            saga_id=saga_id,
            step_name="reserve_inventory",
            action="act",
            status=SagaStepStatus.STARTED,
        )

        history = await storage.get_step_history(saga_id)
        assert len(history) == 1
        assert history[0].saga_id == saga_id
        assert history[0].step_name == "reserve_inventory"
        assert history[0].action == "act"
        assert history[0].status == SagaStepStatus.STARTED
        assert history[0].details is None

    async def test_log_step_act_completed(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test logging act step with COMPLETED status."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        await storage.log_step(
            saga_id=saga_id,
            step_name="process_payment",
            action="act",
            status=SagaStepStatus.COMPLETED,
            details="Payment processed successfully",
        )

        history = await storage.get_step_history(saga_id)
        assert len(history) == 1
        assert history[0].status == SagaStepStatus.COMPLETED
        assert history[0].details == "Payment processed successfully"

    async def test_log_step_compensate(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test logging compensate step."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        await storage.log_step(
            saga_id=saga_id,
            step_name="reserve_inventory",
            action="compensate",
            status=SagaStepStatus.COMPENSATED,
            details="Inventory reservation rolled back",
        )

        history = await storage.get_step_history(saga_id)
        assert len(history) == 1
        assert history[0].action == "compensate"
        assert history[0].status == SagaStepStatus.COMPENSATED
        assert history[0].details == "Inventory reservation rolled back"

    async def test_log_step_multiple_entries(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test logging multiple step entries."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        # Log multiple steps
        await storage.log_step(
            saga_id=saga_id,
            step_name="step1",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await storage.log_step(
            saga_id=saga_id,
            step_name="step1",
            action="act",
            status=SagaStepStatus.COMPLETED,
        )
        await storage.log_step(
            saga_id=saga_id,
            step_name="step2",
            action="act",
            status=SagaStepStatus.STARTED,
        )

        history = await storage.get_step_history(saga_id)
        assert len(history) == 3
        assert history[0].step_name == "step1"
        assert history[0].status == SagaStepStatus.STARTED
        assert history[1].step_name == "step1"
        assert history[1].status == SagaStepStatus.COMPLETED
        assert history[2].step_name == "step2"
        assert history[2].status == SagaStepStatus.STARTED

    async def test_log_step_sets_timestamp(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that log_step sets timestamp on entry."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        before = datetime.datetime.now(datetime.timezone.utc)
        await storage.log_step(
            saga_id=saga_id,
            step_name="test_step",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        after = datetime.datetime.now(datetime.timezone.utc)

        history = await storage.get_step_history(saga_id)
        assert before <= history[0].timestamp <= after

    async def test_log_step_nonexistent_saga_raises_error(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
    ) -> None:
        """Test that logging step for nonexistent saga raises ValueError."""
        with pytest.raises(ValueError, match=f"Saga {saga_id} not found"):
            await storage.log_step(
                saga_id=saga_id,
                step_name="test_step",
                action="act",
                status=SagaStepStatus.STARTED,
            )

    async def test_log_step_all_statuses(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test logging steps with all possible statuses."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        for step_status in SagaStepStatus:
            await storage.log_step(
                saga_id=saga_id,
                step_name=f"step_{step_status.value}",
                action="act",
                status=step_status,
            )

        history = await storage.get_step_history(saga_id)
        assert len(history) == len(SagaStepStatus)
        for i, step_status in enumerate(SagaStepStatus):
            assert history[i].status == step_status


class TestLoadSagaState:
    """Tests for load_saga_state method."""

    async def test_load_saga_state_success(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test successful saga state loading."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.PENDING
        assert context == test_context

    async def test_load_saga_state_after_updates(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test loading saga state after multiple updates."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        new_context = {"updated": "context"}
        await storage.update_context(saga_id=saga_id, context=new_context)
        await storage.update_status(saga_id=saga_id, status=SagaStatus.RUNNING)

        status, context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING
        assert context == new_context

    async def test_load_saga_state_nonexistent_saga_raises_error(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
    ) -> None:
        """Test that loading state for nonexistent saga raises ValueError."""
        with pytest.raises(ValueError, match=f"Saga {saga_id} not found"):
            await storage.load_saga_state(saga_id)


class TestGetStepHistory:
    """Tests for get_step_history method."""

    async def test_get_step_history_empty(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test getting history for saga with no logged steps."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        history = await storage.get_step_history(saga_id)
        assert history == []

    async def test_get_step_history_nonexistent_saga_returns_empty(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
    ) -> None:
        """Test that getting history for nonexistent saga returns empty list."""
        history = await storage.get_step_history(saga_id)
        assert history == []

    async def test_get_step_history_sorted_by_timestamp(
        self,
        storage: MemorySagaStorage,
        saga_id: uuid.UUID,
        test_context: dict[str, str],
    ) -> None:
        """Test that history is sorted by timestamp."""
        await storage.create_saga(
            saga_id=saga_id,
            name="test_saga",
            context=test_context,
        )

        # Log steps with delays to ensure different timestamps
        await storage.log_step(
            saga_id=saga_id,
            step_name="step1",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await asyncio.sleep(0.01)
        await storage.log_step(
            saga_id=saga_id,
            step_name="step2",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await asyncio.sleep(0.01)
        await storage.log_step(
            saga_id=saga_id,
            step_name="step3",
            action="act",
            status=SagaStepStatus.STARTED,
        )

        history = await storage.get_step_history(saga_id)
        assert len(history) == 3
        # Verify timestamps are in ascending order
        for i in range(len(history) - 1):
            assert history[i].timestamp <= history[i + 1].timestamp

    async def test_get_step_history_multiple_sagas_isolation(
        self,
        storage: MemorySagaStorage,
        test_context: dict[str, str],
    ) -> None:
        """Test that step history is isolated per saga."""
        saga_id1 = uuid.uuid4()
        saga_id2 = uuid.uuid4()

        await storage.create_saga(
            saga_id=saga_id1,
            name="saga1",
            context=test_context,
        )
        await storage.create_saga(
            saga_id=saga_id2,
            name="saga2",
            context=test_context,
        )

        await storage.log_step(
            saga_id=saga_id1,
            step_name="step1",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await storage.log_step(
            saga_id=saga_id1,
            step_name="step2",
            action="act",
            status=SagaStepStatus.STARTED,
        )
        await storage.log_step(
            saga_id=saga_id2,
            step_name="step3",
            action="act",
            status=SagaStepStatus.STARTED,
        )

        history1 = await storage.get_step_history(saga_id1)
        history2 = await storage.get_step_history(saga_id2)

        assert len(history1) == 2
        assert len(history2) == 1
        assert all(entry.step_name in ["step1", "step2"] for entry in history1)
        assert history2[0].step_name == "step3"

import abc
import typing
import uuid

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry


class ISagaStorage(abc.ABC):
    """Interface for saga persistence storage."""

    @abc.abstractmethod
    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """Initialize a new saga in storage."""

    @abc.abstractmethod
    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: int | None = None,
    ) -> None:
        """Save saga context snapshot.

        Args:
            saga_id: The ID of the saga to update.
            context: The new context data.
            current_version: The expected current version of the saga execution.
                             If provided, optimistic locking will be used.
        Raises:
            SagaConcurrencyError: If optimistic locking fails.
        """

    @abc.abstractmethod
    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        """Update saga global status."""

    @abc.abstractmethod
    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        """Log a step transition."""

    @abc.abstractmethod
    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        """Load current saga status, context, and version."""

    @abc.abstractmethod
    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        """Get step execution history."""

    @abc.abstractmethod
    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: int | None = None,
    ) -> list[uuid.UUID]:
        """Return saga IDs that need recovery.

        Args:
            limit: Maximum number of saga IDs to return.
            max_recovery_attempts: Only include sagas with recovery_attempts
                strictly less than this value. Default 5.
            stale_after_seconds: If set, only include sagas whose updated_at
                is older than (now_utc - stale_after_seconds). Use this to
                avoid picking sagas that are currently being executed (recently
                updated). None means no staleness filter (backward compatible).

        Returns:
            List of saga IDs (RUNNING or COMPENSATING only; FAILED sagas are
            not included), ordered by updated_at ascending, with
            recovery_attempts < max_recovery_attempts, and optionally
            updated_at older than the staleness threshold.
        """

    @abc.abstractmethod
    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: SagaStatus | None = None,
    ) -> None:
        """Atomically increment recovery attempts after a failed recovery.

        Updates recovery_attempts += 1, updated_at = now(), and optionally
        status. Also increments version for optimistic locking.

        Args:
            saga_id: The saga to update.
            new_status: If provided, set saga status to this value (e.g. FAILED).
        """

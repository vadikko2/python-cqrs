import abc
import typing
import uuid

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry


class ISagaStorage(abc.ABC):
    """Interface for saga persistence storage.

    Storage is responsible for persisting saga execution state so that:
    - Saga progress (status, context, step history) survives process restarts.
    - Recovery jobs can find interrupted sagas (RUNNING/COMPENSATING) and retry them.
    - Optimistic locking (version) prevents lost updates when multiple workers
      touch the same saga.
    """

    @abc.abstractmethod
    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        """Create a new saga record in storage (initial state).

        Called when a saga is started for the first time. Creates the execution
        record with PENDING status, initial context, and version 1.

        Args:
            saga_id: Unique identifier of the saga (used as primary key).
            name: Saga name (e.g. handler/type name) for diagnostics and filtering.
            context: Initial context as a JSON-serializable dict (step inputs/outputs).
        """

    @abc.abstractmethod
    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: int | None = None,
    ) -> None:
        """Save saga context snapshot (e.g. after a step completes).

        Persists the current context so recovery can resume with up-to-date data.
        When current_version is provided, implements optimistic locking: update
        succeeds only if the stored version equals current_version (and version
        is incremented), otherwise a concurrent update is detected.

        Args:
            saga_id: The ID of the saga to update.
            context: The new context data (full snapshot, JSON-serializable).
            current_version: The expected current version of the saga execution.
                If provided, optimistic locking is used; if the stored version
                differs, the update is rejected.

        Raises:
            SagaConcurrencyError: If optimistic locking fails (version mismatch).
        """

    @abc.abstractmethod
    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        """Update the saga's global status.

        Status drives lifecycle: PENDING → RUNNING → COMPLETED, or RUNNING →
        COMPENSATING → FAILED. Used by execution and recovery to know whether
        to run steps, compensate, or consider the saga finished.

        Args:
            saga_id: The ID of the saga to update.
            status: New status (e.g. SagaStatus.RUNNING, SagaStatus.COMPLETED,
                SagaStatus.COMPENSATING, SagaStatus.FAILED).
        """

    @abc.abstractmethod
    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        """Append a step transition to the saga log.

        Used to record each step's outcome (started/completed/failed/compensated)
        so that recovery can determine which steps have already been executed
        and which need to be run or compensated.

        Args:
            saga_id: The ID of the saga this step belongs to.
            step_name: Name of the step (must match the step handler name).
            action: "act" for forward execution, "compensate" for compensation.
            status: Step outcome: STARTED, COMPLETED, FAILED, or COMPENSATED.
            details: Optional message (e.g. error text when status is FAILED).
        """

    @abc.abstractmethod
    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        """Load current saga status, context, and version.

        Used by execution and recovery to restore in-memory state. When
        read_for_update is True, the implementation may lock the row (e.g.
        SELECT FOR UPDATE) to avoid concurrent updates.

        Args:
            saga_id: The ID of the saga to load.
            read_for_update: If True, lock the row for update (e.g. for
                subsequent update_context with optimistic locking).

        Returns:
            Tuple of (status, context_dict, version). context_dict is the
            last persisted context; version is used for optimistic locking.
        """

    @abc.abstractmethod
    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        """Return the ordered list of step log entries for the saga.

        Used by recovery to determine which steps completed successfully
        (and must be compensated in reverse order if compensating) and
        which steps still need to be executed.

        Args:
            saga_id: The ID of the saga whose step history to load.

        Returns:
            List of SagaLogEntry in chronological order (oldest first).
        """

    @abc.abstractmethod
    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: int | None = None,
        saga_name: str | None = None,
    ) -> list[uuid.UUID]:
        """Return saga IDs that are candidates for recovery.

        Used by a recovery job/scheduler to find sagas that were left in
        RUNNING or COMPENSATING (e.g. process crash) and should be retried.
        Excludes COMPLETED and optionally limits by recovery attempts,
        staleness, and saga name to avoid re-processing fresh or repeatedly
        failing sagas.

        Args:
            limit: Maximum number of saga IDs to return per call.
            max_recovery_attempts: Only include sagas with recovery_attempts
                strictly less than this value. Sagas that have failed
                recovery this many times can be excluded (e.g. marked FAILED).
                Default 5.
            stale_after_seconds: If set, only include sagas whose updated_at
                is older than (now_utc - stale_after_seconds). Use this to
                avoid picking sagas that are currently being executed (recently
                updated). None means no staleness filter (backward compatible).
            saga_name: If set, only include sagas with this name (e.g. handler
                or type name). None means return all saga types (default).

        Returns:
            List of saga IDs (RUNNING or COMPENSATING only; FAILED/COMPLETED
            are not included), ordered by updated_at ascending, with
            recovery_attempts < max_recovery_attempts, and optionally
            updated_at older than the staleness threshold and name equal to
            saga_name when saga_name is provided.
        """

    @abc.abstractmethod
    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: SagaStatus | None = None,
    ) -> None:
        """Increment recovery attempt counter after a failed recovery run.

        Called when recovery of a saga fails (e.g. exception). Increments
        recovery_attempts and optionally sets status (e.g. to FAILED) so that
        get_sagas_for_recovery can exclude this saga or limit retries.

        Args:
            saga_id: The saga that failed recovery.
            new_status: If provided, set saga status to this value (e.g.
                SagaStatus.FAILED) in the same atomic update.
        """

    @abc.abstractmethod
    async def set_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        attempts: int,
    ) -> None:
        """Set recovery attempt counter to an explicit value.

        Used to reset the counter after successfully recovering one of the
        steps (e.g. set to 0), or to set it to the maximum value so that
        get_sagas_for_recovery excludes this saga from further recovery
        (e.g. mark as permanently failed without changing status).

        Args:
            saga_id: The saga to update.
            attempts: The value to set recovery_attempts to (e.g. 0 to reset,
                or max_recovery_attempts to exclude from recovery).
        """

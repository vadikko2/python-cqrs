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

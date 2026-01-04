import datetime
import logging
import typing
import uuid

from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger("cqrs.saga.storage.memory")


class MemorySagaStorage(ISagaStorage):
    """In-memory implementation of ISagaStorage for testing and development."""

    def __init__(self) -> None:
        # Structure: {saga_id: {name, status, context, created_at, updated_at, version}}
        self._sagas: dict[uuid.UUID, dict[str, typing.Any]] = {}
        # Structure: {saga_id: [SagaLogEntry, ...]}
        self._logs: dict[uuid.UUID, list[SagaLogEntry]] = {}

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        if saga_id in self._sagas:
            raise ValueError(f"Saga {saga_id} already exists")

        now = datetime.datetime.now(datetime.timezone.utc)
        self._sagas[saga_id] = {
            "name": name,
            "status": SagaStatus.PENDING,
            "context": context,
            "created_at": now,
            "updated_at": now,
            "version": 1,
        }
        self._logs[saga_id] = []

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: int | None = None,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        saga_data = self._sagas[saga_id]

        # Optimistic locking check
        if current_version is not None:
            if saga_data["version"] != current_version:
                raise SagaConcurrencyError(f"Saga {saga_id} was modified concurrently")

        saga_data["context"] = context
        saga_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        saga_data["version"] += 1

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        saga_data = self._sagas[saga_id]
        saga_data["status"] = status
        saga_data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        saga_data["version"] += 1

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        entry = SagaLogEntry(
            saga_id=saga_id,
            step_name=step_name,
            action=action,
            status=status,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            details=details,
        )
        self._logs[saga_id].append(entry)
        logger.debug(f"Saga {saga_id} step {step_name} {action}: {status}")

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")

        data = self._sagas[saga_id]
        return data["status"], data["context"], data["version"]

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        if saga_id not in self._logs:
            return []
        # Sort by timestamp
        return sorted(self._logs[saga_id], key=lambda x: x.timestamp)

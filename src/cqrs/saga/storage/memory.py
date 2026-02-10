import contextlib
import datetime
import logging
import typing
import uuid

from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage, SagaStorageRun

logger = logging.getLogger("cqrs.saga.storage.memory")


class _MemorySagaStorageRun(SagaStorageRun):
    """Run that delegates to the underlying MemorySagaStorage; commit/rollback are no-ops."""

    def __init__(self, storage: "MemorySagaStorage") -> None:
        self._storage = storage

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        await self._storage.create_saga(saga_id, name, context)

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: int | None = None,
    ) -> None:
        await self._storage.update_context(saga_id, context, current_version)

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        await self._storage.update_status(saga_id, status)

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        await self._storage.log_step(
            saga_id,
            step_name,
            action,
            status,
            details,
        )

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        return await self._storage.load_saga_state(
            saga_id,
            read_for_update=read_for_update,
        )

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        return await self._storage.get_step_history(saga_id)

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass


class MemorySagaStorage(ISagaStorage):
    """In-memory implementation of ISagaStorage for testing and development."""

    def __init__(self) -> None:
        # Structure: {saga_id: {name, status, context, created_at, updated_at, version}}
        self._sagas: dict[uuid.UUID, dict[str, typing.Any]] = {}
        # Structure: {saga_id: [SagaLogEntry, ...]}
        self._logs: dict[uuid.UUID, list[SagaLogEntry]] = {}

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        @contextlib.asynccontextmanager
        async def _run() -> typing.AsyncGenerator[SagaStorageRun, None]:
            yield _MemorySagaStorageRun(self)

        return _run()

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
            "recovery_attempts": 0,
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

    async def get_sagas_for_recovery(
        self,
        limit: int,
        max_recovery_attempts: int = 5,
        stale_after_seconds: int | None = None,
        saga_name: str | None = None,
    ) -> list[uuid.UUID]:
        recoverable = (SagaStatus.RUNNING, SagaStatus.COMPENSATING)
        now = datetime.datetime.now(datetime.timezone.utc)
        threshold = (now - datetime.timedelta(seconds=stale_after_seconds)) if stale_after_seconds is not None else None
        candidates = [
            sid
            for sid, data in self._sagas.items()
            if data["status"] in recoverable
            and data.get("recovery_attempts", 0) < max_recovery_attempts
            and (threshold is None or data["updated_at"] < threshold)
            and (saga_name is None or data["name"] == saga_name)
        ]
        candidates.sort(key=lambda sid: self._sagas[sid]["updated_at"])
        return candidates[:limit]

    async def increment_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        new_status: SagaStatus | None = None,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")
        data = self._sagas[saga_id]
        data["recovery_attempts"] = data.get("recovery_attempts", 0) + 1
        data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        data["version"] += 1
        if new_status is not None:
            data["status"] = new_status

    async def set_recovery_attempts(
        self,
        saga_id: uuid.UUID,
        attempts: int,
    ) -> None:
        if saga_id not in self._sagas:
            raise ValueError(f"Saga {saga_id} not found")
        data = self._sagas[saga_id]
        data["recovery_attempts"] = attempts
        data["updated_at"] = datetime.datetime.now(datetime.timezone.utc)
        data["version"] += 1

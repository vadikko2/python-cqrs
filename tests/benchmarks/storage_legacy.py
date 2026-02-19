"""Shared legacy storage classes for benchmark tests (no create_run; commit-per-call path)."""

from __future__ import annotations

import contextlib

from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import SagaStorageRun
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage


class MemorySagaStorageLegacy(MemorySagaStorage):
    """Memory storage without create_run: forces legacy path (commit per call)."""

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """Raise NotImplementedError so benchmarks use the legacy commit-per-call path."""
        raise NotImplementedError("Legacy storage: create_run disabled for benchmark")


class SqlAlchemySagaStorageLegacy(SqlAlchemySagaStorage):
    """SQLAlchemy storage without create_run: forces legacy path (commit per call)."""

    def create_run(
        self,
    ) -> contextlib.AbstractAsyncContextManager[SagaStorageRun]:
        """Raise NotImplementedError so benchmarks use the legacy commit-per-call path."""
        raise NotImplementedError("Legacy storage: create_run disabled for benchmark")

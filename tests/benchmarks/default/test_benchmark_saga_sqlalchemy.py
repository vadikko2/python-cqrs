"""Benchmarks for Saga with SQLAlchemy storage (default Response). Requires DATABASE_DSN.

- Benchmarks named *_run_* use the scoped run path (create_run, checkpoint commits).
- Benchmarks named *_legacy_* use the legacy path (no create_run, commit per storage call).
"""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from cqrs.saga.saga import Saga
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

from ..conftest import SqlAlchemySagaStorageLegacy
from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


def _make_storage(engine, storage_cls):
    """Build saga storage from engine and storage class (shared by legacy benchmarks)."""
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    return storage_cls(session_factory)


@pytest.fixture
def saga_container() -> SagaContainer:
    """
    Create a SagaContainer pre-registered with the standard order saga steps.

    Returns:
        SagaContainer: Container with ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep registered.
    """
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def saga_sqlalchemy(saga_container: SagaContainer) -> Saga[OrderContext]:
    class OrderSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    return OrderSaga()


@pytest.mark.benchmark
def test_benchmark_saga_sqlalchemy_run_full_transaction(
    benchmark,
    saga_sqlalchemy: Saga[OrderContext],
    saga_container: SagaContainer,
    saga_benchmark_loop_and_engine,
):
    """Benchmark full saga transaction with SQLAlchemy storage, scoped run (MySQL)."""
    loop, engine = saga_benchmark_loop_and_engine

    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    storage = SqlAlchemySagaStorage(session_factory)
    context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)

    async def run_transaction() -> None:
        async with saga_sqlalchemy.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))


@pytest.mark.benchmark
def test_benchmark_saga_sqlalchemy_run_single_step(
    benchmark,
    saga_container: SagaContainer,
    saga_benchmark_loop_and_engine,
):
    """Benchmark saga with single step, scoped run (SQLAlchemy storage)."""
    loop, engine = saga_benchmark_loop_and_engine

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()

    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    storage = SqlAlchemySagaStorage(session_factory)
    context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)

    async def run_transaction() -> None:
        """
        Run the saga transaction to completion by iterating over its yielded steps using the configured context, container, and storage.

        This function is used by benchmarks to execute a full saga flow without performing additional work per step.
        """
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))


# ---- Legacy path (no create_run, commit per storage call) ----


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "storage_cls",
    [SqlAlchemySagaStorage, SqlAlchemySagaStorageLegacy],
    ids=["storage", "legacy"],
)
def test_benchmark_saga_sqlalchemy_legacy_full_transaction(
    benchmark,
    saga_sqlalchemy: Saga[OrderContext],
    saga_container: SagaContainer,
    saga_benchmark_loop_and_engine,
    storage_cls,
):
    """Benchmark full saga transaction with SQLAlchemy storage, legacy path (MySQL)."""
    loop, engine = saga_benchmark_loop_and_engine
    storage = _make_storage(engine, storage_cls)
    context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)

    async def run_transaction() -> None:
        async with saga_sqlalchemy.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))


@pytest.mark.benchmark
@pytest.mark.parametrize(
    "storage_cls",
    [SqlAlchemySagaStorage, SqlAlchemySagaStorageLegacy],
    ids=["storage", "legacy"],
)
def test_benchmark_saga_sqlalchemy_legacy_single_step(
    benchmark,
    saga_container: SagaContainer,
    saga_benchmark_loop_and_engine,
    storage_cls,
):
    """Benchmark saga with single step, legacy path (SQLAlchemy storage)."""
    loop, engine = saga_benchmark_loop_and_engine

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()
    storage = _make_storage(engine, storage_cls)
    context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)

    async def run_transaction() -> None:
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))

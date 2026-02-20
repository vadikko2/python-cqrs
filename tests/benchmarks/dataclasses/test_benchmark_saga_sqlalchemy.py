"""Benchmarks for Saga with SQLAlchemy storage (dataclass DCResponse). Requires DATABASE_DSN.

- Benchmarks named *_run_* use the scoped run path (create_run, checkpoint commits).
- Benchmarks named *_legacy_* use the legacy path (no create_run, commit per storage call).
"""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from cqrs.saga.saga import Saga
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

from ..storage_legacy import SqlAlchemySagaStorageLegacy
from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


@pytest.fixture
def saga_container() -> SagaContainer:
    """
    Create and return a SagaContainer pre-registered with the ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep instances.

    Returns:
        SagaContainer: A container with the three steps already registered.
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
def test_benchmark_saga_sqlalchemy_full_transaction(
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
def test_benchmark_saga_sqlalchemy_single_step(
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
        Execute the saga transaction lifecycle by entering the saga's transaction context and iterating its steps to completion.

        This function opens the saga transaction using the surrounding `saga`, `saga_container`, `storage`, and `context`, then consumes the transaction iterator to drive all saga steps to completion.
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
def test_benchmark_saga_sqlalchemy_legacy_full_transaction(
    benchmark,
    saga_sqlalchemy: Saga[OrderContext],
    saga_container: SagaContainer,
    saga_benchmark_loop_and_engine,
):
    """
    Benchmark a full saga transaction using SQLAlchemy storage in legacy mode.

    Runs a complete saga (three-step OrderSaga) against SqlAlchemySagaStorageLegacy, which disables `create_run` so the storage exercises the legacy commit-per-call path. The benchmark executes the saga transaction in the provided event loop and database engine fixture.
    """
    loop, engine = saga_benchmark_loop_and_engine

    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    storage = SqlAlchemySagaStorageLegacy(session_factory)
    context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)

    async def run_transaction() -> None:
        """
        Execute the configured saga transaction and iterate through all its steps to completion.

        This coroutine opens a transaction using the surrounding `saga_sqlalchemy`, `saga_container`, `context`, and `storage` variables and consumes the transaction iterator without performing additional actions.
        """
        async with saga_sqlalchemy.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))


@pytest.mark.benchmark
def test_benchmark_saga_sqlalchemy_legacy_single_step(
    benchmark,
    saga_container: SagaContainer,
    saga_benchmark_loop_and_engine,
):
    """
    Benchmark executing a single-step Saga using legacy SQLAlchemy storage (commit-per-call path).

    Constructs a SingleStepSaga with ReserveInventoryStep, creates a SqlAlchemySagaStorageLegacy backed by the provided engine/session factory, and measures running a full saga transaction (iterating the transaction to completion) using the provided event loop via the benchmark fixture.
    """
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
    storage = SqlAlchemySagaStorageLegacy(session_factory)
    context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)

    async def run_transaction() -> None:
        """
        Execute the saga transaction lifecycle by entering the saga's transaction context and iterating its steps to completion.

        This function opens the saga transaction using the surrounding `saga`, `saga_container`, `storage`, and `context`, then consumes the transaction iterator to drive all saga steps to completion.
        """
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))

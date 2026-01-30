"""Benchmarks for Saga with SQLAlchemy storage (dataclass DCResponse). Requires DATABASE_DSN."""

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from cqrs.saga.saga import Saga
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


@pytest.fixture
def saga_container() -> SagaContainer:
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
    """Benchmark full saga transaction with SQLAlchemy storage (MySQL)."""
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
    """Benchmark saga with single step (SQLAlchemy storage)."""
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
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: loop.run_until_complete(run_transaction()))

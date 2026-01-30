"""Benchmarks for Saga with SQLAlchemy storage (MySQL). Requires DATABASE_DSN (e.g. in CI)."""

import asyncio
import os

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.storage.sqlalchemy import Base, SqlAlchemySagaStorage

from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentResponse,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderResponse,
    ShipOrderStep,
)


@pytest.fixture(scope="module")
def database_dsn() -> str | None:
    """DATABASE_DSN from environment (set in CI by pytest-config.ini)."""
    return os.environ.get("DATABASE_DSN") or None


async def _init_saga_tables(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@pytest.fixture(scope="module")
def saga_engine(database_dsn: str | None):
    """Create async engine for saga storage. Skip if DATABASE_DSN not set."""
    if not database_dsn:
        pytest.skip("DATABASE_DSN not set (MySQL required)")
    engine = create_async_engine(
        database_dsn,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        echo=False,
    )
    asyncio.run(_init_saga_tables(engine))
    yield engine
    asyncio.run(engine.dispose())


@pytest.fixture(scope="module")
def saga_session_factory(saga_engine):
    """Session factory for saga storage."""
    return async_sessionmaker(
        saga_engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


@pytest.fixture
def sqlalchemy_storage(saga_session_factory: async_sessionmaker[AsyncSession]):
    """SqlAlchemySagaStorage instance per test."""
    return SqlAlchemySagaStorage(saga_session_factory)


@pytest.fixture
def saga_container() -> SagaContainer:
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def saga_sqlalchemy(
    saga_container: SagaContainer,
    sqlalchemy_storage: SqlAlchemySagaStorage,
) -> Saga[OrderContext]:
    class OrderSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    return OrderSaga()


@pytest.mark.benchmark
def test_benchmark_saga_sqlalchemy_full_transaction(
    benchmark,
    saga_sqlalchemy: Saga[OrderContext],
    saga_container: SagaContainer,
    sqlalchemy_storage: SqlAlchemySagaStorage,
):
    """Benchmark full saga transaction with SQLAlchemy storage (MySQL)."""

    async def run() -> None:
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga_sqlalchemy.transaction(
            context=context,
            container=saga_container,
            storage=sqlalchemy_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_saga_sqlalchemy_single_step(
    benchmark,
    saga_container: SagaContainer,
    sqlalchemy_storage: SqlAlchemySagaStorage,
):
    """Benchmark saga with single step (SQLAlchemy storage)."""

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()

    async def run() -> None:
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=sqlalchemy_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())

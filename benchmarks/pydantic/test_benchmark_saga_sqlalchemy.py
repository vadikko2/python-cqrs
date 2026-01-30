"""Benchmarks for Saga with SQLAlchemy storage (Pydantic Response). Requires DATABASE_DSN."""

import asyncio
import os

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from cqrs.saga.saga import Saga
from cqrs.saga.storage.sqlalchemy import Base, SqlAlchemySagaStorage

from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


@pytest.fixture(scope="module")
def database_dsn() -> str | None:
    """
    Return the value of the DATABASE_DSN environment variable or None if unset.
    
    Returns:
        str | None: The DATABASE_DSN value from the environment, or None when not present.
    """
    return os.environ.get("DATABASE_DSN") or None


async def _init_saga_tables(engine):
    """
    Initialize the database schema for saga models by dropping any existing tables and creating tables defined on `Base.metadata` within a single transactional connection.
    
    Parameters:
        engine: An async SQLAlchemy Engine used to acquire a connection and execute schema DDL.
    
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@pytest.fixture(scope="module")
def saga_engine(database_dsn: str | None):
    """
    Provide an async SQLAlchemy engine configured for saga storage and initialize saga tables.
    
    When `database_dsn` is None or empty, the test is skipped with a message indicating MySQL is required. This fixture initializes the saga schema before yielding the engine and disposes the engine after use.
    
    Parameters:
        database_dsn (str | None): Database DSN to create the async engine; if None the test is skipped.
    
    Returns:
        engine: An initialized async SQLAlchemy engine configured for saga storage.
    """
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
    """
    Create an async SQLAlchemy session factory bound to the provided engine for saga storage.
    
    Parameters:
        saga_engine: The async SQLAlchemy Engine to bind the session factory to.
    
    Returns:
        An `async_sessionmaker` configured with expire_on_commit=False, autocommit=False, and autoflush=False.
    """
    return async_sessionmaker(
        saga_engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


@pytest.fixture
def sqlalchemy_storage(saga_session_factory: async_sessionmaker[AsyncSession]):
    """
    Create a SqlAlchemySagaStorage configured with the provided async session factory.
    
    This is intended for use as a pytest fixture to supply a storage instance backed
    by the given SQLAlchemy async session factory.
    
    Parameters:
        saga_session_factory (async_sessionmaker[AsyncSession]): Async SQLAlchemy session
            factory that the storage will use to open sessions.
    
    Returns:
        SqlAlchemySagaStorage: A storage instance bound to the provided session factory.
    """
    return SqlAlchemySagaStorage(saga_session_factory)


@pytest.fixture
def saga_container() -> SagaContainer:
    """
    Create and return a SagaContainer with ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep registered.
    
    Returns:
        SagaContainer: A container preconfigured with instances of ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep.
    """
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
    """
    Create an OrderSaga configured with the ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep.
    
    Returns:
        OrderSaga: An instance of the configured saga containing the three steps in execution order.
    """
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
    """
    Benchmark executing a full multi-step Saga transaction using SQLAlchemy-backed storage.
    
    Parameters:
        benchmark: pytest-benchmark fixture used to measure the runtime of the coroutine.
        saga_sqlalchemy: Configured Saga[OrderContext] instance to run.
        saga_container: SagaContainer supplying the registered saga steps.
        sqlalchemy_storage: SqlAlchemySagaStorage instance used as the saga persistence layer.
    """

    async def run() -> None:
        """
        Execute a full OrderSaga transaction using the configured SQLAlchemy storage and SagaContainer.
        
        Creates an OrderContext and runs the saga transaction to completion by iterating the transaction steps.
        """
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
        """
        Execute a full saga transaction for a sample OrderContext, iterating each step to completion.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=sqlalchemy_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())
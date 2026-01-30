"""Benchmarks for Saga with SQLAlchemy storage (dataclass DCResponse). Requires DATABASE_DSN."""

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
    Retrieve the DATABASE_DSN environment variable.
    
    Returns:
        str | None: The value of the DATABASE_DSN environment variable if set, otherwise `None`.
    """
    return os.environ.get("DATABASE_DSN") or None


async def _init_saga_tables(engine):
    """
    Recreate the Saga database schema by dropping all tables defined on Base.metadata and then creating them.
    
    Parameters:
        engine: An asynchronous SQLAlchemy Engine or connection provider used to run the schema operations.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@pytest.fixture(scope="module")
def saga_engine(database_dsn: str | None):
    """
    Provide an async SQLAlchemy engine configured for saga storage, initialize the saga tables, and skip the test if no DATABASE_DSN is provided.
    
    Parameters:
        database_dsn (str | None): Database DSN; if `None`, the fixture will skip the test session.
    
    Returns:
        engine (AsyncEngine): Initialized async SQLAlchemy engine bound for saga storage; engine is disposed on fixture teardown.
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
    Create an async SQLAlchemy session factory configured for saga storage.
    
    Parameters:
        saga_engine (AsyncEngine): Async engine to bind the session factory to.
    
    Returns:
        async_sessionmaker[AsyncSession]: A session factory with expire_on_commit=False, autocommit=False, and autoflush=False.
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
    Create a SqlAlchemySagaStorage configured with the given async session factory.
    
    Parameters:
        saga_session_factory (async_sessionmaker[AsyncSession]): Async SQLAlchemy session factory used to create sessions for storage operations.
    
    Returns:
        SqlAlchemySagaStorage: Storage instance that uses the provided session factory for database access.
    """
    return SqlAlchemySagaStorage(saga_session_factory)


@pytest.fixture
def saga_container() -> SagaContainer:
    """
    Create a SagaContainer pre-registered with the saga step components used by order processing.
    
    The container will have ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep registered and ready for use.
    
    Returns:
        SagaContainer: A container configured with the three registered step instances.
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
    Constructs an OrderSaga instance configured with the standard order processing steps.
    
    Returns:
        Saga[OrderContext]: An instantiated saga whose steps are ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep.
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
    Run and benchmark a full multi-step saga transaction using SQLAlchemy-backed storage.
    
    This test executes a complete transactional run of the provided Saga with the given SagaContainer and SqlAlchemySagaStorage and measures its performance via the pytest-benchmark fixture.
    """

    async def run() -> None:
        """
        Execute a saga transaction using the provided container and SQLAlchemy storage.
        
        Creates an OrderContext and opens a transactional saga execution via `saga_sqlalchemy.transaction`
        with the given `saga_container` and `sqlalchemy_storage`, then iterates the transaction to completion
        without performing side effects.
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
        Execute a full OrderSaga transaction using the provided container and SQLAlchemy storage.
        
        Creates an OrderContext for order_id "ord_1" and runs the saga transaction to completion by iterating over its steps without performing additional actions.
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
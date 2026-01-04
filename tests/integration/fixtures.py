import contextlib
import functools
import os

import dotenv
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from cqrs.outbox import sqlalchemy

dotenv.load_dotenv()
DATABASE_DSN = os.environ.get("DATABASE_DSN", "")


@pytest.fixture(scope="function")
async def init_orm():
    """Initialize outbox tables - drops and creates tables before each test."""
    engine = create_async_engine(
        DATABASE_DSN,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=30,
        echo=False,
    )
    async with engine.begin() as connect:
        await connect.run_sync(sqlalchemy.Base.metadata.drop_all)
        await connect.run_sync(sqlalchemy.Base.metadata.create_all)
        yield connect


@pytest.fixture(scope="function")
async def session(init_orm):
    engine_factory = functools.partial(
        create_async_engine,
        DATABASE_DSN,
        isolation_level="REPEATABLE READ",
    )
    session = async_sessionmaker(engine_factory())()
    async with contextlib.aclosing(session):
        yield session


# Saga storage fixtures
@pytest.fixture(scope="session")
async def init_saga_orm():
    """Initialize saga storage tables - drops and creates tables BEFORE test only."""
    from cqrs.saga.storage.sqlalchemy import Base

    engine = create_async_engine(
        DATABASE_DSN,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=30,
        echo=False,
    )
    # Drop and create tables BEFORE test (not after)
    # Use begin() to ensure tables are created, but don't keep transaction open
    async with engine.begin() as connect:
        await connect.run_sync(Base.metadata.drop_all)
        await connect.run_sync(Base.metadata.create_all)

    # Yield engine so it can be used for sessions
    # Data will persist after test because we don't drop tables in cleanup
    yield engine

    # Cleanup: dispose engine but DON'T drop tables - keep data in DB
    await engine.dispose()


@pytest.fixture(scope="session")
def saga_session_factory(init_saga_orm):
    """Create a session factory for saga storage tests."""
    engine = init_saga_orm
    return async_sessionmaker(engine, expire_on_commit=False, autocommit=False)


@pytest.fixture(scope="session")
async def saga_session(saga_session_factory):
    """Create a session for saga storage tests - commits data to persist."""
    # Use autocommit=False but ensure we commit explicitly
    session = saga_session_factory()

    async with contextlib.aclosing(session):
        try:
            yield session
            # Final commit before closing to ensure data persists
            if session.in_transaction():
                await session.commit()
        except Exception:
            # Only rollback on exception
            if session.in_transaction():
                await session.rollback()
            raise
        # No cleanup that would delete data - data persists in DB

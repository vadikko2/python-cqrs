import pytest
import os
import asyncio
from sqlalchemy.ext import asyncio as sqla_async
import dotenv

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def engine():
    engine = sqla_async.create_async_engine(url=DATABASE_URL, pool_pre_ping=True)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="function")
async def session(engine):
    connection = await engine.connect()
    transaction = await connection.begin()

    session_maker = sqla_async.async_sessionmaker(bind=connection, expire_on_commit=False)
    session = session_maker()

    yield session

    await session.close()
    await transaction.rollback()
    await connection.close()


@pytest.fixture(scope="session")
async def init_saga_orm(engine):
    """Initialize saga storage tables."""
    from cqrs.saga.storage.sqlalchemy import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest.fixture(scope="session")
def saga_session_factory(saga_engine, init_saga_orm):
    return sqla_async.async_sessionmaker(saga_engine, expire_on_commit=False)
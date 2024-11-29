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

"""Shared fixtures for benchmarks. Engine and loop are session-scoped for saga SQLAlchemy benchmarks."""

import asyncio
import os

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from cqrs.saga.storage.sqlalchemy import Base


@pytest.fixture(scope="session")
def database_dsn() -> str | None:
    """DATABASE_DSN from environment (set in CI by pytest-config.ini)."""
    return os.environ.get("DATABASE_DSN") or None


@pytest.fixture(scope="session")
def saga_benchmark_loop_and_engine(database_dsn: str | None):
    """
    One event loop and one async engine for the whole benchmark session.
    Used by saga SQLAlchemy benchmarks so connection setup/teardown is not measured.
    Fails (no skip) if DATABASE_DSN is unset or connection fails, so CI shows the real error.
    """
    if not database_dsn:
        pytest.fail("DATABASE_DSN not set; set it in CI (e.g. in codspeed.yml env) to run saga SQLAlchemy benchmarks")

    loop = asyncio.new_event_loop()
    engine = create_async_engine(
        database_dsn,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=4,
        echo=False,
    )

    async def ensure_tables() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    loop.run_until_complete(ensure_tables())

    try:
        yield (loop, engine)
    finally:
        loop.run_until_complete(engine.dispose())
        loop.close()

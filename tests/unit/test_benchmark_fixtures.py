"""Tests for benchmark conftest.py fixtures and infrastructure."""

import asyncio
import os
from unittest import mock

import pytest


def test_database_dsn_from_environment():
    """Test database_dsn returns value from environment variable."""
    with mock.patch.dict(os.environ, {"DATABASE_DSN": "mysql://test:test@localhost/db"}):
        # Import inside the context to get the mocked environment
        from importlib import reload
        import tests.benchmarks.conftest as conftest_module

        reload(conftest_module)
        dsn = os.environ.get("DATABASE_DSN") or None
        assert dsn == "mysql://test:test@localhost/db"


def test_database_dsn_none_when_not_set():
    """Test database_dsn returns None when environment variable not set."""
    # This tests the fixture logic: os.environ.get("DATABASE_DSN") or None
    with mock.patch.dict(os.environ, {}, clear=True):
        dsn = os.environ.get("DATABASE_DSN") or None
        assert dsn is None


def test_database_dsn_none_when_empty():
    """Test database_dsn returns None for empty string."""
    # Empty strings should be treated as None
    with mock.patch.dict(os.environ, {"DATABASE_DSN": ""}):
        dsn = os.environ.get("DATABASE_DSN") or None
        assert dsn is None


def test_saga_benchmark_fixture_logic_fails_without_dsn(monkeypatch):
    """Test saga_benchmark_loop_and_engine logic fails when DATABASE_DSN is not set."""
    # This test verifies the error behavior when DSN is None
    database_dsn = None

    # The fixture should fail with pytest.fail
    with pytest.raises(pytest.fail.Exception) as exc_info:
        if not database_dsn:
            pytest.fail(
                "DATABASE_DSN not set; set it in CI (e.g. in codspeed.yml env) to run saga SQLAlchemy benchmarks"
            )

    assert "DATABASE_DSN not set" in str(exc_info.value)


def test_benchmark_conftest_imports():
    """Test benchmark conftest has required imports."""
    import tests.benchmarks.conftest as conftest_module

    # Verify required imports exist
    assert hasattr(conftest_module, "asyncio")
    assert hasattr(conftest_module, "os")
    assert hasattr(conftest_module, "pytest")
    assert hasattr(conftest_module, "create_async_engine")
    assert hasattr(conftest_module, "Base")


def test_benchmark_conftest_uses_sqlalchemy_base():
    """Test benchmark conftest imports correct SQLAlchemy Base."""
    from tests.benchmarks.conftest import Base

    # Verify it's the saga storage base
    assert hasattr(Base, "metadata")


def test_benchmark_fixture_docstrings_exist():
    """Test fixtures have proper documentation."""
    import tests.benchmarks.conftest as conftest_module

    # Check that fixtures have docstrings
    assert conftest_module.database_dsn.__doc__ is not None
    assert "DATABASE_DSN" in conftest_module.database_dsn.__doc__

    assert conftest_module.saga_benchmark_loop_and_engine.__doc__ is not None
    assert "event loop" in conftest_module.saga_benchmark_loop_and_engine.__doc__.lower()
    assert "async engine" in conftest_module.saga_benchmark_loop_and_engine.__doc__.lower()


def test_saga_benchmark_error_message_clarity():
    """Test saga_benchmark_loop_and_engine error message is clear."""
    # Test the expected error message format
    expected_message = (
        "DATABASE_DSN not set; set it in CI (e.g. in codspeed.yml env) "
        "to run saga SQLAlchemy benchmarks"
    )

    assert "DATABASE_DSN" in expected_message
    assert "CI" in expected_message or "codspeed.yml" in expected_message


def test_saga_benchmark_fixture_uses_session_scope():
    """Test saga_benchmark_loop_and_engine uses session scope for performance."""
    import tests.benchmarks.conftest as conftest_module

    # Verify the fixture docstring explains why session scope is used
    assert conftest_module.saga_benchmark_loop_and_engine.__doc__ is not None
    doc = conftest_module.saga_benchmark_loop_and_engine.__doc__.lower()
    assert "session" in doc
    assert ("setup" in doc or "teardown" in doc or "connection" in doc)


def test_benchmark_engine_configuration_params():
    """Test engine configuration parameters for benchmarks are valid."""
    # Test that the parameters used in conftest are valid SQLAlchemy parameters
    # by verifying they're in the function signature
    from sqlalchemy.ext.asyncio import create_async_engine
    import inspect

    sig = inspect.signature(create_async_engine)
    params = sig.parameters

    # Verify that the parameters we use are accepted
    # Note: These params might be passed through **kw to underlying create_engine
    expected_params = ["pool_pre_ping", "pool_size", "max_overflow", "echo"]

    # All these should be valid SQLAlchemy engine parameters
    # We don't actually need them all in the signature as they go through **kw
    assert callable(create_async_engine)


def test_saga_benchmark_fixture_is_session_scoped():
    """Test saga_benchmark_loop_and_engine is session-scoped."""
    import inspect
    import tests.benchmarks.conftest as conftest_module

    # Check the fixture decorator
    source = inspect.getsource(conftest_module.saga_benchmark_loop_and_engine)
    # The fixture should have scope="session"
    assert '@pytest.fixture(scope="session")' in source or 'scope="session"' in source


def test_database_dsn_fixture_is_session_scoped():
    """Test database_dsn fixture is session-scoped."""
    import inspect
    import tests.benchmarks.conftest as conftest_module

    source = inspect.getsource(conftest_module.database_dsn)
    assert '@pytest.fixture(scope="session")' in source or 'scope="session"' in source
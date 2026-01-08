import pytest
import redis
from aiobreaker.storage.memory import CircuitMemoryStorage
from aiobreaker.storage.redis import CircuitRedisStorage
from aiobreaker import CircuitBreakerState


@pytest.fixture
def memory_storage_factory():
    def _factory(name: str):
        return CircuitMemoryStorage(state=CircuitBreakerState.CLOSED)

    return _factory


@pytest.fixture
def redis_client():
    """
    Creates a real synchronous Redis client connected to the local instance.
    Skips tests if Redis is not available.
    """
    # aiobreaker's CircuitRedisStorage uses synchronous Redis client methods
    # decode_responses=False is critical because aiobreaker tries to .decode('utf-8') the state
    client = redis.from_url(
        "redis://localhost:6379",
        encoding="utf-8",
        decode_responses=False,
    )
    try:
        client.ping()
    except Exception as e:
        client.close()
        pytest.skip(
            f"Redis is not available: {e}. Make sure 'docker-compose up' is running.",
        )

    yield client

    # Clean up
    try:
        client.flushall()
    except Exception:
        pass
    client.close()


@pytest.fixture
def redis_storage_factory(redis_client):
    def _factory(name: str):
        return CircuitRedisStorage(
            state=CircuitBreakerState.CLOSED,
            redis_object=redis_client,
            namespace=name,
        )

    return _factory

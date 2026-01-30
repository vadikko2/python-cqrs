"""Benchmarks for serialization and deserialization (Pydantic Request/Response)."""

import cqrs
import pytest


class SampleRequest(cqrs.Request):
    field1: str
    field2: int
    field3: list[str]
    field4: dict[str, int]


class SampleResponse(cqrs.Response):
    result: str
    data: dict[str, str]


@pytest.mark.benchmark
def test_benchmark_request_to_dict(benchmark):
    """Benchmark request serialization to dictionary."""
    request = SampleRequest(
        field1="test_value",
        field2=42,
        field3=["a", "b", "c"],
        field4={"key1": 1, "key2": 2},
    )

    benchmark(lambda: request.to_dict())


@pytest.mark.benchmark
def test_benchmark_request_from_dict(benchmark):
    """
    Benchmark deserialization of a SampleRequest from a plain dictionary.
    
    Runs the pytest-benchmark `benchmark` fixture against constructing a SampleRequest
    via SampleRequest.from_dict using representative sample data for all fields.
    
    Parameters:
        benchmark: The pytest-benchmark fixture used to measure execution time.
    """
    data = {
        "field1": "test_value",
        "field2": 42,
        "field3": ["a", "b", "c"],
        "field4": {"key1": 1, "key2": 2},
    }

    benchmark(lambda: SampleRequest.from_dict(**data))


@pytest.mark.benchmark
def test_benchmark_response_to_dict(benchmark):
    """
    Measure the performance of serializing a SampleResponse instance to a dictionary.
    """
    response = SampleResponse(
        result="success",
        data={"key1": "value1", "key2": "value2"},
    )

    benchmark(lambda: response.to_dict())


@pytest.mark.benchmark
def test_benchmark_response_from_dict(benchmark):
    """
    Benchmark deserialization of a SampleResponse object from a dictionary.
    
    Uses the pytest-benchmark fixture to measure the performance of SampleResponse.from_dict(**data).
    
    Parameters:
        benchmark: pytest-benchmark fixture used to run the benchmark.
    """
    data = {
        "result": "success",
        "data": {"key1": "value1", "key2": "value2"},
    }

    benchmark(lambda: SampleResponse.from_dict(**data))


@pytest.mark.benchmark
def test_benchmark_complex_nested_structure(benchmark):
    """Benchmark serialization of complex nested structures."""

    class NestedRequest(cqrs.Request):
        level1: dict[str, list[dict[str, str]]]
        level2: list[dict[str, int]]

    request = NestedRequest(
        level1={
            "group1": [{"name": "item1", "value": "val1"}] * 5,
            "group2": [{"name": "item2", "value": "val2"}] * 5,
        },
        level2=[{"counter": i} for i in range(10)],
    )

    benchmark(lambda: request.to_dict())
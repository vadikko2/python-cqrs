"""Benchmarks for serialization and deserialization (dataclass DCRequest/DCResponse)."""

import dataclasses

import cqrs
import pytest


@dataclasses.dataclass
class SampleRequest(cqrs.DCRequest):
    field1: str
    field2: int
    field3: list[str]
    field4: dict[str, int]


@dataclasses.dataclass
class SampleResponse(cqrs.DCResponse):
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
    """Benchmark request deserialization from dictionary."""
    data = {
        "field1": "test_value",
        "field2": 42,
        "field3": ["a", "b", "c"],
        "field4": {"key1": 1, "key2": 2},
    }

    benchmark(lambda: SampleRequest.from_dict(**data))


@pytest.mark.benchmark
def test_benchmark_response_to_dict(benchmark):
    """Benchmark response serialization to dictionary."""
    response = SampleResponse(
        result="success",
        data={"key1": "value1", "key2": "value2"},
    )

    benchmark(lambda: response.to_dict())


@pytest.mark.benchmark
def test_benchmark_response_from_dict(benchmark):
    """Benchmark response deserialization from dictionary."""
    data = {
        "result": "success",
        "data": {"key1": "value1", "key2": "value2"},
    }

    benchmark(lambda: SampleResponse.from_dict(**data))


@pytest.mark.benchmark
def test_benchmark_complex_nested_structure(benchmark):
    """Benchmark serialization of complex nested structures."""

    @dataclasses.dataclass
    class NestedRequest(cqrs.DCRequest):
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

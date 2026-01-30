"""Benchmarks for StreamingRequestHandler (dataclass DCRequest/DCResponse)."""

import dataclasses
import typing

import cqrs
import di
import pytest
from cqrs.events.event import IEvent
from cqrs.requests import bootstrap
from cqrs.requests.request_handler import StreamingRequestHandler
from cqrs.response import DCResponse


@dataclasses.dataclass
class ProcessItemsCommand(cqrs.DCRequest):
    item_ids: list[str]


@dataclasses.dataclass
class ProcessItemResult(DCResponse):
    item_id: str
    status: str


class StreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
    def __init__(self) -> None:
        """
        Initialize the handler and prepare an empty internal events list.
        
        Creates an empty private list used to record produced IEvent instances.
        """
        self._events: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Provide a snapshot of the handler's recorded events.
        
        Returns:
            A shallow copy of the sequence of `IEvent` objects that have been recorded by the handler.
        """
        return self._events.copy()

    def clear_events(self) -> None:
        """
        Clear all recorded notification events from the handler's internal event list.
        """
        self._events.clear()

    async def handle(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        """
        Stream results for each item in the request and record a NotificationEvent for every processed item.
        
        This async generator iterates request.item_ids, appends a `cqrs.NotificationEvent` with `event_name="ItemProcessed"` and payload `{"item_id": item_id}` to the handler's internal events list for each item, and yields a `ProcessItemResult` with status "processed".
        
        Parameters:
            request (ProcessItemsCommand): Command containing the list of item IDs to process.
        
        Returns:
            AsyncIterator[ProcessItemResult]: An async iterator that yields a `ProcessItemResult` for each item in `request.item_ids`.
        """
        for item_id in request.item_ids:
            self._events.append(
                cqrs.NotificationEvent(
                    event_name="ItemProcessed",
                    payload={"item_id": item_id},
                ),
            )
            yield ProcessItemResult(item_id=item_id, status="processed")


def streaming_mapper(mapper: cqrs.RequestMap) -> None:
    """
    Register the streaming handler for ProcessItemsCommand in the given request map.
    
    Parameters:
        mapper (cqrs.RequestMap): The request map to which ProcessItemsCommand is bound to StreamingHandler.
    """
    mapper.bind(ProcessItemsCommand, StreamingHandler)


@pytest.fixture
def streaming_mediator():
    """
    Create a bootstrap-configured streaming mediator for tests.
    
    Returns:
        A streaming mediator instance configured with a new dependency-injection container and the module's command mapper.
    """
    return bootstrap.bootstrap_streaming(
        di_container=di.Container(),
        commands_mapper=streaming_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_stream_single_item(streaming_mediator, benchmark):
    """Benchmark streaming handler with single item."""

    async def run():
        """
        Stream a single-item ProcessItemsCommand through the mediator and collect its results.
        
        Returns:
            list[ProcessItemResult]: A list of ProcessItemResult produced by streaming the request.
        """
        request = ProcessItemsCommand(item_ids=["item_1"])
        results = []
        async for result in streaming_mediator.stream(request):
            results.append(result)
        return results

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_stream_ten_items(streaming_mediator, benchmark):
    """Benchmark streaming handler with 10 items."""

    async def run():
        """
        Send a ProcessItemsCommand with ten item IDs to the streaming mediator and collect all streamed results.
        
        Returns:
            list[ProcessItemResult]: List of ProcessItemResult objects produced by the stream, one per processed item.
        """
        request = ProcessItemsCommand(
            item_ids=[f"item_{i}" for i in range(10)],
        )
        results = []
        async for result in streaming_mediator.stream(request):
            results.append(result)
        return results

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_stream_hundred_items(streaming_mediator, benchmark):
    """Benchmark streaming handler with 100 items."""

    async def run():
        """
        Send a ProcessItemsCommand for 100 items to the streaming mediator and collect all streamed results.
        
        Returns:
            list[ProcessItemResult]: Collected ProcessItemResult objects, one per processed item.
        """
        request = ProcessItemsCommand(
            item_ids=[f"item_{i}" for i in range(100)],
        )
        results = []
        async for result in streaming_mediator.stream(request):
            results.append(result)
        return results

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_stream_ten_requests_five_items_each(streaming_mediator, benchmark):
    """Benchmark 10 streaming requests with 5 items each."""

    async def run():
        """
        Run ten streaming requests that each process five items and exhaust their streams.
        
        Each iteration creates a ProcessItemsCommand with item IDs formatted as "item_{i}_{j}" for j in 0..4 and iterates through the mediator's stream to consume all results.
        """
        for i in range(10):
            request = ProcessItemsCommand(
                item_ids=[f"item_{i}_{j}" for j in range(5)],
            )
            async for _ in streaming_mediator.stream(request):
                pass

    benchmark(lambda: run())
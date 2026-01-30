"""Benchmarks for StreamingRequestHandler (Pydantic Request/Response)."""

import typing

import cqrs
import di
import pytest
from cqrs.events.event import IEvent
from cqrs.requests import bootstrap
from cqrs.requests.request_handler import StreamingRequestHandler


class ProcessItemsCommand(cqrs.Request):
    item_ids: list[str]


class ProcessItemResult(cqrs.Response):
    item_id: str
    status: str


class StreamingHandler(StreamingRequestHandler[ProcessItemsCommand, ProcessItemResult]):
    def __init__(self) -> None:
        """
        Initialize the handler and prepare internal storage for emitted events.
        
        The handler maintains a private list used to collect IEvent instances produced while processing requests.
        """
        self._events: list[IEvent] = []

    @property
    def events(self) -> typing.Sequence[IEvent]:
        """
        Get a shallow copy of the handler's recorded events.
        
        Returns:
            A sequence of IEvent objects representing a shallow copy of the handler's recorded events.
        """
        return self._events.copy()

    def clear_events(self) -> None:
        """
        Clear all events recorded by the handler.
        
        Resets the internal event list so the handler has no stored events after the call.
        """
        self._events.clear()

    async def handle(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        request: ProcessItemsCommand,
    ) -> typing.AsyncIterator[ProcessItemResult]:
        """
        Stream processing results for each item in the request and record a NotificationEvent for each processed item.
        
        Each processed item appends a NotificationEvent with name "ItemProcessed" and payload {"item_id": <id>} to the handler's internal events list.
        
        Returns:
            ProcessItemResult objects, one per item_id from the request, each with `item_id` set to the item identifier and `status` set to "processed".
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
    Register the StreamingHandler for ProcessItemsCommand on the provided request map.
    
    Parameters:
        mapper (cqrs.RequestMap): Request map to bind ProcessItemsCommand to StreamingHandler.
    """
    mapper.bind(ProcessItemsCommand, StreamingHandler)


@pytest.fixture
def streaming_mediator():
    """
    Create and return a streaming mediator configured for the module's streaming handlers.
    
    Returns:
        A streaming mediator instance configured with a new dependency-injection container and the module's streaming mapper.
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
        Execute a streaming request for a single item and collect all streamed results.
        
        Returns:
        	results (list[ProcessItemResult]): Collected streamed results for the request, in the order they were received.
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
        Stream-processes ten items using the streaming mediator and collects the emitted results.
        
        Creates a ProcessItemsCommand with item IDs "item_0" through "item_9", streams the handler responses from `streaming_mediator.stream`, and returns all received ProcessItemResult objects.
        
        Returns:
            list[ProcessItemResult]: Collected results for the ten processed items.
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
        Execute a streaming request for 100 items and collect all results.
        
        Returns:
            list[ProcessItemResult]: A list of ProcessItemResult objects, one per streamed item.
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
        Execute ten streaming requests, each processing five items, and consume all streamed results.
        
        Creates a ProcessItemsCommand for i in 0..9 with item_ids ["item_i_0", ..., "item_i_4"] and streams each request through the shared streaming_mediator, discarding every yielded ProcessItemResult.
        """
        for i in range(10):
            request = ProcessItemsCommand(
                item_ids=[f"item_{i}_{j}" for j in range(5)],
            )
            async for _ in streaming_mediator.stream(request):
                pass

    benchmark(lambda: run())
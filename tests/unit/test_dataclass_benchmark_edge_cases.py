"""Edge case tests for dataclass-based benchmarks."""

import asyncio
import dataclasses

import cqrs
import di
import pytest
from cqrs.events import bootstrap as events_bootstrap
from cqrs.requests import bootstrap as requests_bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.requests.request_handler import StreamingRequestHandler


class TestCorRequestHandlerEdgeCases:
    """Edge case tests for Chain of Responsibility with dataclasses."""

    async def test_cor_handler_with_empty_request(self):
        """Test CoR handler can process request with no fields."""

        @dataclasses.dataclass
        class EmptyRequest(cqrs.DCRequest):
            """Request with no fields."""

            pass

        @dataclasses.dataclass
        class EmptyResponse(cqrs.DCResponse):
            """Response with no fields."""

            pass

        class EmptyHandler(CORRequestHandler[EmptyRequest, EmptyResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: EmptyRequest):
                return EmptyResponse()

        def mapper(m: cqrs.RequestMap):
            m.bind(EmptyRequest, [EmptyHandler])

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        result = await mediator.send(EmptyRequest())
        assert isinstance(result, EmptyResponse)

    async def test_cor_chain_all_handlers_return_none(self):
        """Test CoR chain when all handlers return None."""

        @dataclasses.dataclass
        class TestRequest(cqrs.DCRequest):
            value: int

        class Handler1(CORRequestHandler[TestRequest, int | None]):
            @property
            def events(self):
                return []

            async def handle(self, request: TestRequest):
                return await self.next(request)

        class Handler2(CORRequestHandler[TestRequest, int | None]):
            @property
            def events(self):
                return []

            async def handle(self, request: TestRequest):
                return await self.next(request)

        class Handler3(CORRequestHandler[TestRequest, int | None]):
            @property
            def events(self):
                return []

            async def handle(self, request: TestRequest):
                # Last handler can only return None
                return None

        def mapper(m: cqrs.RequestMap):
            m.bind(TestRequest, [Handler1, Handler2, Handler3])

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        result = await mediator.send(TestRequest(value=42))
        assert result is None

    async def test_cor_handler_with_large_data(self):
        """Test CoR handler with large data payload."""

        @dataclasses.dataclass
        class LargeRequest(cqrs.DCRequest):
            data: list[str]

        @dataclasses.dataclass
        class LargeResponse(cqrs.DCResponse):
            count: int

        class LargeDataHandler(CORRequestHandler[LargeRequest, LargeResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: LargeRequest):
                return LargeResponse(count=len(request.data))

        def mapper(m: cqrs.RequestMap):
            m.bind(LargeRequest, [LargeDataHandler])

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        # Test with 1000 items
        large_data = [f"item_{i}" for i in range(1000)]
        result = await mediator.send(LargeRequest(data=large_data))
        assert result.count == 1000


class TestEventHandlingEdgeCases:
    """Edge case tests for event handling with dataclasses."""

    @dataclasses.dataclass(frozen=True)
    class EmptyEvent(cqrs.DCEvent):
        """Event with no fields."""

        pass

    async def test_event_handler_with_empty_event(self):
        """Test event handler can process event with no fields."""

        class EmptyEventHandler(cqrs.EventHandler[self.EmptyEvent]):
            def __init__(self):
                self.called = False

            async def handle(self, event: self.EmptyEvent):
                self.called = True

        def mapper(m: cqrs.EventMap):
            m.bind(self.EmptyEvent, EmptyEventHandler)

        mediator = events_bootstrap.bootstrap(
            di_container=di.Container(),
            events_mapper=mapper,
        )

        await mediator.send(self.EmptyEvent())
        # Verify it was processed without error

    async def test_multiple_handlers_for_same_event(self):
        """Test multiple handlers processing the same event."""

        @dataclasses.dataclass(frozen=True)
        class MultiHandlerEvent(cqrs.DCEvent):
            value: int

        class Handler1(cqrs.EventHandler[MultiHandlerEvent]):
            def __init__(self):
                self.values = []

            async def handle(self, event: MultiHandlerEvent):
                self.values.append(event.value)

        class Handler2(cqrs.EventHandler[MultiHandlerEvent]):
            def __init__(self):
                self.values = []

            async def handle(self, event: MultiHandlerEvent):
                self.values.append(event.value * 2)

        def mapper(m: cqrs.EventMap):
            m.bind(MultiHandlerEvent, Handler1)
            m.bind(MultiHandlerEvent, Handler2)

        mediator = events_bootstrap.bootstrap(
            di_container=di.Container(),
            events_mapper=mapper,
        )

        await mediator.send(MultiHandlerEvent(value=5))
        # Both handlers should process the event

    async def test_event_with_complex_nested_data(self):
        """Test event handling with deeply nested data structures."""

        @dataclasses.dataclass(frozen=True)
        class NestedEvent(cqrs.DCEvent):
            nested: dict[str, list[dict[str, int]]]

        class NestedEventHandler(cqrs.EventHandler[NestedEvent]):
            def __init__(self):
                self.processed_event = None

            async def handle(self, event: NestedEvent):
                self.processed_event = event

        def mapper(m: cqrs.EventMap):
            m.bind(NestedEvent, NestedEventHandler)

        mediator = events_bootstrap.bootstrap(
            di_container=di.Container(),
            events_mapper=mapper,
        )

        complex_data = {
            "level1": [{"counter": 1}, {"counter": 2}],
            "level2": [{"value": 10}, {"value": 20}],
        }
        await mediator.send(NestedEvent(nested=complex_data))


class TestRequestHandlingEdgeCases:
    """Edge case tests for request handling with dataclasses."""

    async def test_command_with_none_response(self):
        """Test command handler that explicitly returns None."""

        @dataclasses.dataclass
        class VoidCommand(cqrs.DCRequest):
            action: str

        class VoidCommandHandler(cqrs.RequestHandler[VoidCommand, None]):
            @property
            def events(self):
                return []

            async def handle(self, request: VoidCommand):
                return None

        def mapper(m: cqrs.RequestMap):
            m.bind(VoidCommand, VoidCommandHandler)

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        result = await mediator.send(VoidCommand(action="do_something"))
        assert result is None

    async def test_query_with_empty_result(self):
        """Test query handler returning empty collection."""

        @dataclasses.dataclass
        class EmptyQuery(cqrs.DCRequest):
            filter_id: str

        @dataclasses.dataclass
        class EmptyResult(cqrs.DCResponse):
            items: list[str]

        class EmptyQueryHandler(cqrs.RequestHandler[EmptyQuery, EmptyResult]):
            @property
            def events(self):
                return []

            async def handle(self, request: EmptyQuery):
                return EmptyResult(items=[])

        def mapper(m: cqrs.RequestMap):
            m.bind(EmptyQuery, EmptyQueryHandler)

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            queries_mapper=mapper,
        )

        result = await mediator.send(EmptyQuery(filter_id="missing"))
        assert result.items == []

    async def test_concurrent_command_execution(self):
        """Test multiple commands executed concurrently."""

        @dataclasses.dataclass
        class ConcurrentCommand(cqrs.DCRequest):
            id: int

        class ConcurrentCommandHandler(cqrs.RequestHandler[ConcurrentCommand, None]):
            def __init__(self):
                self.processed_ids = []

            @property
            def events(self):
                return []

            async def handle(self, request: ConcurrentCommand):
                await asyncio.sleep(0.01)  # Simulate async work
                self.processed_ids.append(request.id)

        def mapper(m: cqrs.RequestMap):
            m.bind(ConcurrentCommand, ConcurrentCommandHandler)

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        # Execute 10 commands concurrently
        tasks = [mediator.send(ConcurrentCommand(id=i)) for i in range(10)]
        await asyncio.gather(*tasks)


class TestStreamingHandlerEdgeCases:
    """Edge case tests for streaming request handlers with dataclasses."""

    async def test_streaming_handler_empty_stream(self):
        """Test streaming handler that yields no items."""

        @dataclasses.dataclass
        class EmptyStreamRequest(cqrs.DCRequest):
            should_yield: bool

        @dataclasses.dataclass
        class StreamResult(cqrs.DCResponse):
            value: str

        class EmptyStreamHandler(StreamingRequestHandler[EmptyStreamRequest, StreamResult]):
            def __init__(self):
                self._events = []

            @property
            def events(self):
                return self._events

            def clear_events(self):
                self._events.clear()

            async def handle(self, request: EmptyStreamRequest):  # pyright: ignore[reportIncompatibleMethodOverride]
                if request.should_yield:
                    yield StreamResult(value="item")
                # Yield nothing if should_yield is False

        def mapper(m: cqrs.RequestMap):
            m.bind(EmptyStreamRequest, EmptyStreamHandler)

        mediator = requests_bootstrap.bootstrap_streaming(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        results = []
        async for result in mediator.stream(EmptyStreamRequest(should_yield=False)):
            results.append(result)

        assert results == []

    async def test_streaming_handler_single_item(self):
        """Test streaming handler yielding exactly one item."""

        @dataclasses.dataclass
        class SingleItemRequest(cqrs.DCRequest):
            pass

        @dataclasses.dataclass
        class StreamResult(cqrs.DCResponse):
            value: int

        class SingleItemHandler(StreamingRequestHandler[SingleItemRequest, StreamResult]):
            def __init__(self):
                self._events = []

            @property
            def events(self):
                return self._events

            def clear_events(self):
                self._events.clear()

            async def handle(self, request: SingleItemRequest):  # pyright: ignore[reportIncompatibleMethodOverride]
                yield StreamResult(value=1)

        def mapper(m: cqrs.RequestMap):
            m.bind(SingleItemRequest, SingleItemHandler)

        mediator = requests_bootstrap.bootstrap_streaming(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        results = []
        async for result in mediator.stream(SingleItemRequest()):
            results.append(result)

        assert len(results) == 1
        assert results[0].value == 1

    async def test_streaming_handler_large_stream(self):
        """Test streaming handler with large number of items."""

        @dataclasses.dataclass
        class LargeStreamRequest(cqrs.DCRequest):
            count: int

        @dataclasses.dataclass
        class StreamResult(cqrs.DCResponse):
            index: int

        class LargeStreamHandler(StreamingRequestHandler[LargeStreamRequest, StreamResult]):
            def __init__(self):
                self._events = []

            @property
            def events(self):
                return self._events

            def clear_events(self):
                self._events.clear()

            async def handle(self, request: LargeStreamRequest):  # pyright: ignore[reportIncompatibleMethodOverride]
                for i in range(request.count):
                    yield StreamResult(index=i)

        def mapper(m: cqrs.RequestMap):
            m.bind(LargeStreamRequest, LargeStreamHandler)

        mediator = requests_bootstrap.bootstrap_streaming(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        results = []
        async for result in mediator.stream(LargeStreamRequest(count=1000)):
            results.append(result)

        assert len(results) == 1000
        assert results[0].index == 0
        assert results[-1].index == 999


class TestSerializationEdgeCases:
    """Edge case tests for serialization with dataclasses."""

    def test_serialization_with_none_values(self):
        """Test serialization of dataclass with None values."""

        @dataclasses.dataclass
        class OptionalFieldRequest(cqrs.DCRequest):
            required: str
            optional: str | None = None

        request = OptionalFieldRequest(required="test", optional=None)
        data = request.to_dict()

        assert data["required"] == "test"
        assert "optional" in data
        assert data["optional"] is None

        # Deserialize back
        restored = OptionalFieldRequest.from_dict(**data)
        assert restored.required == "test"
        assert restored.optional is None

    def test_serialization_with_empty_collections(self):
        """Test serialization of dataclass with empty collections."""

        @dataclasses.dataclass
        class CollectionRequest(cqrs.DCRequest):
            empty_list: list[str]
            empty_dict: dict[str, int]

        request = CollectionRequest(empty_list=[], empty_dict={})
        data = request.to_dict()

        assert data["empty_list"] == []
        assert data["empty_dict"] == {}

        restored = CollectionRequest.from_dict(**data)
        assert restored.empty_list == []
        assert restored.empty_dict == {}

    def test_serialization_with_special_characters(self):
        """Test serialization with special characters in strings."""

        @dataclasses.dataclass
        class SpecialCharsRequest(cqrs.DCRequest):
            text: str

        special_texts = [
            "Hello\nWorld",
            "Tab\tSeparated",
            'Quote"Test',
            "Unicode: 你好",
            "Emoji: 🚀",
        ]

        for text in special_texts:
            request = SpecialCharsRequest(text=text)
            data = request.to_dict()
            restored = SpecialCharsRequest.from_dict(**data)
            assert restored.text == text

    def test_serialization_deeply_nested_structure(self):
        """Test serialization with very deep nesting."""

        @dataclasses.dataclass
        class DeepRequest(cqrs.DCRequest):
            level1: dict[str, dict[str, dict[str, list[dict[str, str]]]]]

        deep_data = {
            "a": {
                "b": {
                    "c": [
                        {"d": "value1"},
                        {"e": "value2"},
                    ]
                }
            }
        }

        request = DeepRequest(level1=deep_data)
        data = request.to_dict()
        restored = DeepRequest.from_dict(**data)

        assert restored.level1["a"]["b"]["c"][0]["d"] == "value1"
        assert restored.level1["a"]["b"]["c"][1]["e"] == "value2"

    def test_serialization_with_numeric_edge_cases(self):
        """Test serialization with numeric edge cases."""

        @dataclasses.dataclass
        class NumericRequest(cqrs.DCRequest):
            zero: int
            negative: int
            large: int
            float_val: float

        request = NumericRequest(
            zero=0,
            negative=-999,
            large=2**31 - 1,
            float_val=3.14159,
        )

        data = request.to_dict()
        restored = NumericRequest.from_dict(**data)

        assert restored.zero == 0
        assert restored.negative == -999
        assert restored.large == 2**31 - 1
        assert abs(restored.float_val - 3.14159) < 0.00001
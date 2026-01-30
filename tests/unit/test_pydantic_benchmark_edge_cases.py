"""Edge case tests for Pydantic-based benchmarks."""

import asyncio

import cqrs
import di
import pytest
from cqrs.events import bootstrap as events_bootstrap
from cqrs.requests import bootstrap as requests_bootstrap
from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.requests.request_handler import StreamingRequestHandler
from pydantic import Field, field_validator


class TestCorRequestHandlerPydanticEdgeCases:
    """Edge case tests for Chain of Responsibility with Pydantic models."""

    async def test_cor_handler_with_pydantic_validation(self):
        """Test CoR handler with Pydantic field validation."""

        class ValidatedRequest(cqrs.Request):
            age: int = Field(ge=0, le=150)
            email: str = Field(pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")

        class ValidatedResponse(cqrs.Response):
            valid: bool

        class ValidatingHandler(CORRequestHandler[ValidatedRequest, ValidatedResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: ValidatedRequest):
                return ValidatedResponse(valid=True)

        def mapper(m: cqrs.RequestMap):
            m.bind(ValidatedRequest, [ValidatingHandler])

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        result = await mediator.send(ValidatedRequest(age=25, email="test@example.com"))
        assert result.valid is True

    async def test_cor_handler_with_default_values(self):
        """Test CoR handler with Pydantic default values."""

        class RequestWithDefaults(cqrs.Request):
            required_field: str
            optional_field: str = "default_value"
            optional_int: int = 42

        class DefaultResponse(cqrs.Response):
            values: dict[str, str | int]

        class DefaultHandler(CORRequestHandler[RequestWithDefaults, DefaultResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: RequestWithDefaults):
                return DefaultResponse(
                    values={
                        "required": request.required_field,
                        "optional": request.optional_field,
                        "int": request.optional_int,
                    }
                )

        def mapper(m: cqrs.RequestMap):
            m.bind(RequestWithDefaults, [DefaultHandler])

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        # Test with only required field
        result = await mediator.send(RequestWithDefaults(required_field="test"))
        assert result.values["optional"] == "default_value"
        assert result.values["int"] == 42

    async def test_cor_handler_with_nested_pydantic_models(self):
        """Test CoR handler with nested Pydantic models."""

        class NestedModel(cqrs.Response):
            value: str
            count: int

        class NestedRequest(cqrs.Request):
            data: NestedModel

        class NestedResponse(cqrs.Response):
            processed: NestedModel

        class NestedHandler(CORRequestHandler[NestedRequest, NestedResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: NestedRequest):
                return NestedResponse(processed=request.data)

        def mapper(m: cqrs.RequestMap):
            m.bind(NestedRequest, [NestedHandler])

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        nested = NestedModel(value="test", count=5)
        result = await mediator.send(NestedRequest(data=nested))
        assert result.processed.value == "test"
        assert result.processed.count == 5


class TestEventHandlingPydanticEdgeCases:
    """Edge case tests for event handling with Pydantic models."""

    async def test_frozen_event_immutability(self):
        """Test that frozen Pydantic events are immutable."""

        class ImmutableEvent(cqrs.Event, frozen=True):
            user_id: str
            timestamp: int

        event = ImmutableEvent(user_id="user_1", timestamp=123456)

        # Verify event is frozen
        with pytest.raises((AttributeError, ValueError, TypeError)):
            event.user_id = "user_2"  # type: ignore

    async def test_event_with_custom_validators(self):
        """Test event with custom Pydantic validators."""

        class ValidatedEvent(cqrs.Event, frozen=True):
            value: int

            @field_validator("value")
            @classmethod
            def validate_positive(cls, v: int) -> int:
                if v <= 0:
                    raise ValueError("Value must be positive")
                return v

        # Valid event
        event = ValidatedEvent(value=10)
        assert event.value == 10

        # Invalid event should raise validation error
        with pytest.raises(ValueError):
            ValidatedEvent(value=-5)

    async def test_event_with_list_of_models(self):
        """Test event containing list of Pydantic models."""

        class Item(cqrs.Response):
            id: str
            name: str

        class BatchEvent(cqrs.Event, frozen=True):
            items: list[Item]

        class BatchEventHandler(cqrs.EventHandler[BatchEvent]):
            def __init__(self):
                self.item_count = 0

            async def handle(self, event: BatchEvent):
                self.item_count = len(event.items)

        def mapper(m: cqrs.EventMap):
            m.bind(BatchEvent, BatchEventHandler)

        mediator = events_bootstrap.bootstrap(
            di_container=di.Container(),
            events_mapper=mapper,
        )

        items = [
            Item(id="1", name="Item 1"),
            Item(id="2", name="Item 2"),
            Item(id="3", name="Item 3"),
        ]
        await mediator.send(BatchEvent(items=items))


class TestRequestHandlingPydanticEdgeCases:
    """Edge case tests for request handling with Pydantic models."""

    async def test_command_with_field_aliases(self):
        """Test command with Pydantic field aliases."""

        class AliasedCommand(cqrs.Request):
            internal_name: str = Field(alias="externalName")

        class AliasedResponse(cqrs.Response):
            result: str

        class AliasedHandler(cqrs.RequestHandler[AliasedCommand, AliasedResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: AliasedCommand):
                return AliasedResponse(result=request.internal_name)

        def mapper(m: cqrs.RequestMap):
            m.bind(AliasedCommand, AliasedHandler)

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        # Create with alias
        result = await mediator.send(AliasedCommand(**{"externalName": "test_value"}))
        assert result.result == "test_value"

    async def test_query_with_computed_fields(self):
        """Test query response with computed/derived fields."""

        class ComputedQuery(cqrs.Request):
            value: int

        class ComputedResponse(cqrs.Response):
            value: int
            doubled: int

            def __init__(self, **data):
                if "doubled" not in data:
                    data["doubled"] = data.get("value", 0) * 2
                super().__init__(**data)

        class ComputedHandler(cqrs.RequestHandler[ComputedQuery, ComputedResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: ComputedQuery):
                return ComputedResponse(value=request.value)

        def mapper(m: cqrs.RequestMap):
            m.bind(ComputedQuery, ComputedHandler)

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            queries_mapper=mapper,
        )

        result = await mediator.send(ComputedQuery(value=21))
        assert result.value == 21
        assert result.doubled == 42

    async def test_request_with_union_types(self):
        """Test request with union type fields."""

        class UnionRequest(cqrs.Request):
            value: int | str | None

        class UnionResponse(cqrs.Response):
            type_name: str

        class UnionHandler(cqrs.RequestHandler[UnionRequest, UnionResponse]):
            @property
            def events(self):
                return []

            async def handle(self, request: UnionRequest):
                return UnionResponse(type_name=type(request.value).__name__)

        def mapper(m: cqrs.RequestMap):
            m.bind(UnionRequest, UnionHandler)

        mediator = requests_bootstrap.bootstrap(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        # Test with int
        result = await mediator.send(UnionRequest(value=42))
        assert result.type_name == "int"

        # Test with str
        result = await mediator.send(UnionRequest(value="hello"))
        assert result.type_name == "str"

        # Test with None
        result = await mediator.send(UnionRequest(value=None))
        assert result.type_name == "NoneType"


class TestStreamingHandlerPydanticEdgeCases:
    """Edge case tests for streaming request handlers with Pydantic models."""

    async def test_streaming_with_validated_responses(self):
        """Test streaming handler with validated response models."""

        class StreamRequest(cqrs.Request):
            count: int = Field(ge=0, le=100)

        class ValidatedStreamResult(cqrs.Response):
            index: int = Field(ge=0)
            value: str

        class ValidatedStreamHandler(StreamingRequestHandler[StreamRequest, ValidatedStreamResult]):
            def __init__(self):
                self._events = []

            @property
            def events(self):
                return self._events

            def clear_events(self):
                self._events.clear()

            async def handle(self, request: StreamRequest):  # pyright: ignore[reportIncompatibleMethodOverride]
                for i in range(request.count):
                    yield ValidatedStreamResult(index=i, value=f"item_{i}")

        def mapper(m: cqrs.RequestMap):
            m.bind(StreamRequest, ValidatedStreamHandler)

        mediator = requests_bootstrap.bootstrap_streaming(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        results = []
        async for result in mediator.stream(StreamRequest(count=5)):
            results.append(result)
            assert result.index >= 0
            assert result.value.startswith("item_")

        assert len(results) == 5

    async def test_streaming_with_complex_models(self):
        """Test streaming handler with complex nested Pydantic models."""

        class NestedData(cqrs.Response):
            id: str
            metadata: dict[str, str | int]

        class ComplexStreamRequest(cqrs.Request):
            prefix: str

        class ComplexStreamResult(cqrs.Response):
            data: NestedData

        class ComplexStreamHandler(StreamingRequestHandler[ComplexStreamRequest, ComplexStreamResult]):
            def __init__(self):
                self._events = []

            @property
            def events(self):
                return self._events

            def clear_events(self):
                self._events.clear()

            async def handle(self, request: ComplexStreamRequest):  # pyright: ignore[reportIncompatibleMethodOverride]
                for i in range(3):
                    nested = NestedData(
                        id=f"{request.prefix}_{i}",
                        metadata={"index": i, "type": "test"},
                    )
                    yield ComplexStreamResult(data=nested)

        def mapper(m: cqrs.RequestMap):
            m.bind(ComplexStreamRequest, ComplexStreamHandler)

        mediator = requests_bootstrap.bootstrap_streaming(
            di_container=di.Container(),
            commands_mapper=mapper,
        )

        results = []
        async for result in mediator.stream(ComplexStreamRequest(prefix="item")):
            results.append(result)

        assert len(results) == 3
        assert results[0].data.id == "item_0"
        assert results[0].data.metadata["index"] == 0


class TestSerializationPydanticEdgeCases:
    """Edge case tests for serialization with Pydantic models."""

    def test_pydantic_model_config_options(self):
        """Test Pydantic models with custom config options."""

        class StrictRequest(cqrs.Request):
            model_config = {"strict": True}
            value: int

        # Strict mode should reject string for int
        with pytest.raises(ValueError):
            StrictRequest(value="not_an_int")  # type: ignore

    def test_serialization_with_exclude_fields(self):
        """Test serialization with excluded fields."""

        class ExcludeFieldRequest(cqrs.Request):
            public_field: str
            private_field: str = Field(exclude=True, default="secret")

        request = ExcludeFieldRequest(public_field="visible", private_field="hidden")
        data = request.to_dict()

        assert "public_field" in data
        assert "private_field" not in data

    def test_serialization_with_optional_fields(self):
        """Test serialization with optional/nullable fields."""

        class OptionalRequest(cqrs.Request):
            required: str
            optional: str | None = None

        # With value
        request1 = OptionalRequest(required="test", optional="value")
        data1 = request1.to_dict()
        assert data1["optional"] == "value"

        # Without value (None)
        request2 = OptionalRequest(required="test", optional=None)
        data2 = request2.to_dict()
        assert "optional" in data2
        assert data2["optional"] is None

    def test_deserialization_with_extra_fields(self):
        """Test deserialization behavior with extra fields."""

        class StrictRequest(cqrs.Request):
            model_config = {"extra": "forbid"}
            field1: str

        # Should raise error with extra fields
        with pytest.raises(ValueError):
            StrictRequest.from_dict(field1="value", extra_field="should_fail")

    def test_serialization_with_datetime_fields(self):
        """Test serialization with datetime fields."""
        from datetime import datetime

        class DateTimeRequest(cqrs.Request):
            timestamp: datetime

        now = datetime.now()
        request = DateTimeRequest(timestamp=now)
        data = request.to_dict()

        # Pydantic should serialize datetime to ISO format string
        assert "timestamp" in data
        assert isinstance(data["timestamp"], str) or isinstance(data["timestamp"], datetime)

    def test_serialization_with_enum_fields(self):
        """Test serialization with enum fields."""
        from enum import Enum

        class Status(str, Enum):
            PENDING = "pending"
            ACTIVE = "active"
            COMPLETED = "completed"

        class EnumRequest(cqrs.Request):
            status: Status

        request = EnumRequest(status=Status.ACTIVE)
        data = request.to_dict()

        assert "status" in data
        # Enum should be serialized to its value
        assert data["status"] in ["active", Status.ACTIVE]

    def test_deserialization_with_type_coercion(self):
        """Test Pydantic type coercion during deserialization."""

        class CoercionRequest(cqrs.Request):
            int_field: int
            bool_field: bool
            list_field: list[int]

        # Pydantic should coerce types
        request = CoercionRequest.from_dict(
            int_field="42",  # String to int
            bool_field=1,  # Int to bool
            list_field=[1, "2", 3],  # Mixed to ints
        )

        assert request.int_field == 42
        assert request.bool_field is True
        assert request.list_field == [1, 2, 3]

    def test_serialization_round_trip_with_complex_data(self):
        """Test complete serialization round trip with complex data."""

        class ComplexRequest(cqrs.Request):
            strings: list[str]
            numbers: list[int]
            mapping: dict[str, int | str]
            nested: dict[str, list[dict[str, str]]]

        original = ComplexRequest(
            strings=["a", "b", "c"],
            numbers=[1, 2, 3],
            mapping={"key1": 100, "key2": "value"},
            nested={"level1": [{"nested_key": "nested_value"}]},
        )

        # Serialize
        data = original.to_dict()

        # Deserialize
        restored = ComplexRequest.from_dict(**data)

        # Verify
        assert restored.strings == original.strings
        assert restored.numbers == original.numbers
        assert restored.mapping == original.mapping
        assert restored.nested == original.nested
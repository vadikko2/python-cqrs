import typing
import uuid
from unittest.mock import Mock, patch

import orjson
import pydantic

import cqrs
from cqrs.deserializers import json, protobuf


class DeserializedModelPayload(pydantic.BaseModel):
    foo: typing.Text
    bar: int


# ============================================================================
# JsonDeserializer Tests
# ============================================================================


def test_json_deserializer_from_none_positive():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent,
    )

    assert deserializer(None) is None


def test_json_deserializer_from_bytes_positive():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    events = cqrs.NotificationEvent[DeserializedModelPayload](
        event_name="empty_event",
        payload=DeserializedModelPayload(foo="foo", bar=1),
    )

    assert deserializer(orjson.dumps(events.model_dump(mode="json"))) == events


def test_json_deserializer_from_str_positive():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    events = cqrs.NotificationEvent[DeserializedModelPayload](
        event_name="empty_event",
        payload=DeserializedModelPayload(foo="foo", bar=1),
    )

    assert deserializer(orjson.dumps(events.model_dump(mode="json")).decode()) == events


def test_json_deserializer_invalid_json_negative():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    result = deserializer("not json")

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == "not json"


def test_json_deserializer_invalid_structure_negative():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    # JSON with missing required field 'event_name'
    invalid_json = '{"invalid_field": "value"}'
    result = deserializer(invalid_json)

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == invalid_json


def test_json_deserializer_missing_required_fields_negative():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    # JSON with payload that has wrong type for required field 'bar' (string instead of int)
    # This should cause a validation error when Pydantic tries to validate the payload
    incomplete_json = (
        '{"event_name": "test", "payload": {"foo": "bar", "bar": "not_an_int"}}'
    )
    result = deserializer(incomplete_json)

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == incomplete_json


def test_json_deserializer_empty_string_negative():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    result = deserializer("")

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == ""


def test_json_deserializer_empty_json_object_negative():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    result = deserializer("{}")

    assert isinstance(result, json.DeserializeJsonError)
    assert result.error_message is not None
    assert result.error_type is not None
    assert result.message_data == "{}"


# ============================================================================
# ProtobufValueDeserializer Tests
# ============================================================================


class MockProtobufMessage:
    """Mock protobuf message for testing."""

    def __init__(self, event_id: str, event_name: str, payload: dict | None = None):
        self.event_id = event_id
        self.event_name = event_name
        self.event_timestamp = "2024-01-01T00:00:00"
        if payload:
            self.payload = Mock()
            self.payload.user_id = payload.get("user_id", "")
            self.payload.meeting_id = payload.get("meeting_id", "")


def test_protobuf_deserializer_success():
    """Test successful protobuf deserialization."""
    # Use a mock class that can be used as a type
    mock_protobuf_model = type("MockProtobufModel", (), {})  # type: ignore[assignment]
    mock_event_model = cqrs.NotificationEvent[DeserializedModelPayload]

    deserializer = protobuf.ProtobufValueDeserializer(
        model=mock_event_model,
        protobuf_model=mock_protobuf_model,  # type: ignore[arg-type]
    )

    mock_proto_message = MockProtobufMessage(
        event_id="123",
        event_name="test_event",
    )

    # Mock the ProtobufDeserializer class to return a callable that returns our mock message
    mock_protobuf_deserializer_instance = Mock(return_value=mock_proto_message)

    with patch(
        "cqrs.deserializers.protobuf.protobuf.ProtobufDeserializer",
        return_value=mock_protobuf_deserializer_instance,
    ):
        # Mock model_validate to return a proper event
        expected_event = cqrs.NotificationEvent[DeserializedModelPayload](
            event_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            event_name="test_event",
            payload=DeserializedModelPayload(foo="foo", bar=1),
        )

        with patch.object(
            mock_event_model,
            "model_validate",
            return_value=expected_event,
        ):
            result = deserializer(b"test_bytes")

            assert isinstance(result, cqrs.NotificationEvent)
            assert result.event_name == "test_event"
            # Verify that ProtobufDeserializer was called correctly
            mock_protobuf_deserializer_instance.assert_called_once_with(
                b"test_bytes",
                None,
            )


def test_protobuf_deserializer_protobuf_deserialization_error():
    """Test error during protobuf deserialization."""
    mock_protobuf_model = type("MockProtobufModel", (), {})  # type: ignore[assignment]
    mock_event_model = cqrs.NotificationEvent[DeserializedModelPayload]

    deserializer = protobuf.ProtobufValueDeserializer(
        model=mock_event_model,
        protobuf_model=mock_protobuf_model,  # type: ignore[arg-type]
    )

    # Mock ProtobufDeserializer to raise an exception
    mock_protobuf_deserializer_instance = Mock(
        side_effect=ValueError("Invalid protobuf data"),
    )

    with patch(
        "cqrs.deserializers.protobuf.protobuf.ProtobufDeserializer",
        return_value=mock_protobuf_deserializer_instance,
    ):
        test_bytes = b"invalid_protobuf_data"
        result = deserializer(test_bytes)

        assert isinstance(result, protobuf.DeserializeProtobufError)
        assert result.error_message == "Invalid protobuf data"
        assert result.error_type is ValueError
        assert result.message_data == test_bytes


def test_protobuf_deserializer_empty_message():
    """Test handling of empty protobuf message."""
    mock_protobuf_model = type("MockProtobufModel", (), {})  # type: ignore[assignment]
    mock_event_model = cqrs.NotificationEvent[DeserializedModelPayload]

    deserializer = protobuf.ProtobufValueDeserializer(
        model=mock_event_model,
        protobuf_model=mock_protobuf_model,  # type: ignore[arg-type]
    )

    # Mock ProtobufDeserializer to return None (empty message)
    mock_protobuf_deserializer_instance = Mock(return_value=None)

    with patch(
        "cqrs.deserializers.protobuf.protobuf.ProtobufDeserializer",
        return_value=mock_protobuf_deserializer_instance,
    ):
        test_bytes = b"empty_message"
        result = deserializer(test_bytes)

        assert isinstance(result, protobuf.DeserializeProtobufError)
        assert "empty" in result.error_message.lower()
        assert result.error_type is ValueError
        assert result.message_data == test_bytes


def test_protobuf_deserializer_validation_error():
    """Test pydantic validation error during model conversion."""
    mock_protobuf_model = type("MockProtobufModel", (), {})  # type: ignore[assignment]
    mock_event_model = cqrs.NotificationEvent[DeserializedModelPayload]

    deserializer = protobuf.ProtobufValueDeserializer(
        model=mock_event_model,
        protobuf_model=mock_protobuf_model,  # type: ignore[arg-type]
    )

    mock_proto_message = MockProtobufMessage(
        event_id="123",
        event_name="test_event",
    )

    mock_protobuf_deserializer_instance = Mock(return_value=mock_proto_message)

    with patch(
        "cqrs.deserializers.protobuf.protobuf.ProtobufDeserializer",
        return_value=mock_protobuf_deserializer_instance,
    ):
        # Create a validation error
        validation_error = pydantic.ValidationError.from_exception_data(
            "TestModel",
            [{"type": "missing", "loc": ("payload",), "input": {}}],
        )

        with patch.object(
            mock_event_model,
            "model_validate",
            side_effect=validation_error,
        ):
            test_bytes = b"test_bytes"
            result = deserializer(test_bytes)

            assert isinstance(result, protobuf.DeserializeProtobufError)
            assert result.error_message is not None
            assert result.error_type == pydantic.ValidationError
            assert result.message_data == test_bytes


def test_protobuf_deserializer_generic_exception():
    """Test handling of generic exceptions during protobuf deserialization."""
    mock_protobuf_model = type("MockProtobufModel", (), {})  # type: ignore[assignment]
    mock_event_model = cqrs.NotificationEvent[DeserializedModelPayload]

    deserializer = protobuf.ProtobufValueDeserializer(
        model=mock_event_model,
        protobuf_model=mock_protobuf_model,  # type: ignore[arg-type]
    )

    # Mock ProtobufDeserializer to raise a RuntimeError
    mock_protobuf_deserializer_instance = Mock(
        side_effect=RuntimeError("Unexpected error"),
    )

    with patch(
        "cqrs.deserializers.protobuf.protobuf.ProtobufDeserializer",
        return_value=mock_protobuf_deserializer_instance,
    ):
        test_bytes = b"test_bytes"
        result = deserializer(test_bytes)

        assert isinstance(result, protobuf.DeserializeProtobufError)
        assert result.error_message == "Unexpected error"
        assert result.error_type is RuntimeError
        assert result.message_data == test_bytes


def test_protobuf_deserializer_byte_string_input():
    """Test that deserializer accepts ByteString types."""
    mock_protobuf_model = type("MockProtobufModel", (), {})  # type: ignore[assignment]
    mock_event_model = cqrs.NotificationEvent[DeserializedModelPayload]

    deserializer = protobuf.ProtobufValueDeserializer(
        model=mock_event_model,
        protobuf_model=mock_protobuf_model,  # type: ignore[arg-type]
    )

    mock_proto_message = MockProtobufMessage(
        event_id="123",
        event_name="test_event",
    )

    mock_protobuf_deserializer_instance = Mock(return_value=mock_proto_message)

    with patch(
        "cqrs.deserializers.protobuf.protobuf.ProtobufDeserializer",
        return_value=mock_protobuf_deserializer_instance,
    ):
        expected_event = cqrs.NotificationEvent[DeserializedModelPayload](
            event_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            event_name="test_event",
            payload=DeserializedModelPayload(foo="foo", bar=1),
        )

        with patch.object(
            mock_event_model,
            "model_validate",
            return_value=expected_event,
        ):
            # Test with bytes
            result_bytes = deserializer(b"test_bytes")
            assert isinstance(result_bytes, cqrs.NotificationEvent)

            # Reset mock for next call
            mock_protobuf_deserializer_instance.reset_mock()

            # Test with bytearray
            result_bytearray = deserializer(bytearray(b"test_bytes"))
            assert isinstance(result_bytearray, cqrs.NotificationEvent)

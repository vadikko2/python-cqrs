import typing

import orjson
import pydantic

import cqrs
from cqrs.deserializers import json


class DeserializedModelPayload(pydantic.BaseModel):
    foo: typing.Text
    bar: int


def test_json_deserializer_from_none_positive():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent,
    )

    assert deserializer.deserialize(None) is None


def test_json_deserializer_from_bytes_positive():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    events = cqrs.NotificationEvent[DeserializedModelPayload](
        event_name="empty_event",
        payload=DeserializedModelPayload(foo="foo", bar=1),
    )

    assert (
        deserializer.deserialize(orjson.dumps(events.model_dump(mode="json"))) == events
    )


def test_json_deserializer_from_str_positive():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    events = cqrs.NotificationEvent[DeserializedModelPayload](
        event_name="empty_event",
        payload=DeserializedModelPayload(foo="foo", bar=1),
    )

    assert (
        deserializer.deserialize(orjson.dumps(events.model_dump(mode="json")).decode())
        == events
    )


def test_deserializer_negative():
    deserializer = json.JsonDeserializer[cqrs.NotificationEvent](
        model=cqrs.NotificationEvent[DeserializedModelPayload],
    )

    result = deserializer.deserialize("not json")

    assert result is None

import typing

import orjson


def default_serializer(message: typing.Any) -> typing.ByteString:
    """
    Default serializer for messages.

    Works with any object that has a to_dict() method (interface-based approach).
    Falls back to model_dump() if available, otherwise serializes as-is.

    Args:
        message: Object to serialize. Should implement to_dict() method.

    Returns:
        Serialized message as bytes.
    """
    if hasattr(message, "to_dict"):
        return orjson.dumps(message.to_dict())
    elif hasattr(message, "model_dump"):
        return orjson.dumps(message.model_dump(mode="json"))
    else:
        return orjson.dumps(message)

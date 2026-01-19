import logging
import typing

import orjson

from cqrs.deserializers.exceptions import DeserializeJsonError

logger = logging.getLogger("cqrs")


class Deserializable(typing.Protocol):
    """
    Protocol for objects that can be deserialized from a dictionary.

    Objects implementing this protocol must have a classmethod `from_dict`
    that creates an instance from keyword arguments.
    """

    @classmethod
    def from_dict(cls, **kwargs) -> typing.Self:
        """
        Create an instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the object fields.

        Returns:
            A new instance of the class.
        """
        ...


_T = typing.TypeVar("_T", bound=Deserializable)


class JsonDeserializer(typing.Generic[_T]):
    """
    Deserializer for JSON messages.

    Converts JSON strings or bytes into Python objects using the `from_dict`
    classmethod of the target model.

    Example::

        deserializer = JsonDeserializer(MyEvent)
        result = deserializer('{"field": "value"}')
        if isinstance(result, DeserializeJsonError):
            # Handle error
        else:
            # Use result
    """

    def __init__(self, model: typing.Type[typing.Any]):
        """
        Initialize JSON deserializer.

        Args:
            model: Class that has a from_dict classmethod (implements Deserializable protocol
                   or has from_dict method like Pydantic models).
        """
        self._model: typing.Type[typing.Any] = model

    def __call__(self, data: str | bytes | None) -> _T | None | DeserializeJsonError:
        """
        Deserialize JSON data into model instance.

        Args:
            data: JSON string, bytes, or None

        Returns:
            Instance of the model, None if data is None, or DeserializeJsonError on failure.
        """
        if data is None:
            return None
        try:
            json_dict = orjson.loads(data)
            return self._model.from_dict(**json_dict)
        except Exception as e:
            logger.error(
                f"Error while deserializing json message: {e}",
            )
            return DeserializeJsonError(
                error_message=str(e),
                error_type=type(e),
                message_data=data,
            )

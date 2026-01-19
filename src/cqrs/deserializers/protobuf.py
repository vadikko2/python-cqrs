import logging
import typing

import cqrs
from confluent_kafka.schema_registry import protobuf
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message

from cqrs.deserializers.exceptions import DeserializeProtobufError

logger = logging.getLogger("cqrs")


class ProtobufValueDeserializer:
    """
    Deserialize protobuf message into CQRS event model.

    Converts Protobuf binary messages into Python objects using the `from_dict`
    classmethod of the target model.

    Example::

        deserializer = ProtobufValueDeserializer(
            model=MyEvent,
            protobuf_model=MyEventProtobuf
        )
        result = deserializer(binary_protobuf_data)
        if isinstance(result, DeserializeProtobufError):
            # Handle error
        else:
            # Use result
    """

    def __init__(
        self,
        model: typing.Type[cqrs.INotificationEvent],
        protobuf_model: typing.Type[Message],
    ):
        """
        Initialize Protobuf deserializer.

        Args:
            model: Class that implements Deserializable protocol with from_dict method.
            protobuf_model: Protobuf message class for deserialization.
        """
        self._model = model
        self._protobuf_model = protobuf_model

    def __call__(
        self,
        msg: typing.ByteString,
    ) -> cqrs.INotificationEvent | DeserializeProtobufError:
        """
        Deserialize Protobuf binary data into model instance.

        Args:
            msg: Binary Protobuf message data

        Returns:
            Instance of the model or DeserializeProtobufError on failure.
        """
        protobuf_deserializer = protobuf.ProtobufDeserializer(
            self._protobuf_model,
            {"use.deprecated.format": False},
        )
        try:
            proto_event = protobuf_deserializer(msg, None)
        except Exception as error:
            logger.error(
                f"Error while deserializing protobuf message: {error}",
            )
            return DeserializeProtobufError(
                error_message=str(error),
                error_type=type(error),
                message_data=bytes(msg),
            )

        if proto_event is None:
            logger.debug("Protobuf message is empty")
            empty_error = ValueError("Protobuf message is empty")
            return DeserializeProtobufError(
                error_message=str(empty_error),
                error_type=type(empty_error),
                message_data=bytes(msg),
            )

        try:
            # Convert protobuf message to dict
            proto_dict = MessageToDict(proto_event)
            # Use from_dict interface method for validation and type conversion
            return self._model.from_dict(**proto_dict)
        except Exception as error:
            logger.error(
                f"Error while converting proto event to model {self._model.__name__}: {error}",
            )
            return DeserializeProtobufError(
                error_message=str(error),
                error_type=type(error),
                message_data=bytes(msg),
            )

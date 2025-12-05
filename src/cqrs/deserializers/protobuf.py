import logging
import typing

import cqrs
import pydantic
from confluent_kafka.schema_registry import protobuf
from google.protobuf.message import Message

logger = logging.getLogger("cqrs")


class DeserializeProtobufError(pydantic.BaseModel):
    error_message: str
    error_type: typing.Type[Exception]
    message_data: bytes


class ProtobufValueDeserializer:
    """
    Deserialize protobuf message into CQRS event model.
    """

    def __init__(
        self,
        model: typing.Type[cqrs.NotificationEvent],
        protobuf_model: typing.Type[Message],
    ):
        self._model = model
        self._protobuf_model = protobuf_model

    def __call__(
        self,
        msg: typing.ByteString,
    ) -> cqrs.NotificationEvent | DeserializeProtobufError:
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
            return self._model.model_validate(proto_event)
        except pydantic.ValidationError as error:
            logger.error(
                f"Error while validate proto event into model {self._model.__name__}: {error}",
            )
            return DeserializeProtobufError(
                error_message=str(error),
                error_type=type(error),
                message_data=bytes(msg),
            )

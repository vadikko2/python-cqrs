import logging
import typing

import pydantic
from confluent_kafka.schema_registry import protobuf
from google.protobuf.message import Message

import cqrs

logger = logging.getLogger("cqrs")


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
    ) -> cqrs.NotificationEvent | None:
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
            return

        if proto_event is None:
            logger.debug("Protobuf message is empty")
            return

        try:
            return self._model.model_validate(proto_event)
        except pydantic.ValidationError as error:
            logger.error(
                f"Error while deserializing protobuf message: {error}",
            )
            return

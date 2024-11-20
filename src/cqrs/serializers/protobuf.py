import os
import typing

import dotenv
from confluent_kafka import schema_registry, serialization
from confluent_kafka.schema_registry import protobuf

import cqrs

dotenv.load_dotenv()

KAFKA_SCHEMA_REGISTRY_URL = os.getenv(
    "KAFKA_SCHEMA_REGISTRY_URL",
    "http://localhost:8085",
)


def protobuf_value_serializer(
    event: cqrs.NotificationEvent,
) -> typing.ByteString | None:
    """
    Serialize CQRS event model into protobuf message.
    """
    protobuf_event = event.proto()
    schema_registry_client = schema_registry.SchemaRegistryClient(
        {"url": KAFKA_SCHEMA_REGISTRY_URL},
    )
    protobuf_serializer = protobuf.ProtobufSerializer(
        protobuf_event.__class__,
        schema_registry_client,
        {"use.deprecated.format": False},
    )

    context = serialization.SerializationContext(
        event.topic,
        serialization.MessageField.VALUE,
    )

    return protobuf_serializer(protobuf_event, context)

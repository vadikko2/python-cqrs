import asyncio
import typing

import pydantic
from confluent_kafka import schema_registry, serialization
from confluent_kafka.schema_registry import protobuf

import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka
from examples.proto.user_joined_pb2 import UserJoinedECST  # type: ignore


class UserJoinedNotificationPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedECSTPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str


TOPIC_NAME = "user_joined_proto"
SUBJECT_NAME = f"{TOPIC_NAME}-value"
# Настройка Schema Registry
schema_registry_client = schema_registry.SchemaRegistryClient(
    {"url": "http://localhost:8085"},
)
protobuf_serializer = protobuf.ProtobufSerializer(
    UserJoinedECST,
    schema_registry_client,
    {"use.deprecated.format": False},
)


def serializer(
    event: cqrs.ECSTEvent[UserJoinedECSTPayload],
) -> typing.ByteString | None:
    proto_event = UserJoinedECST(
        event_id=str(event.event_id),
        event_timestamp=str(event.event_timestamp),
        event_name=event.event_name,
        event_type=event.event_type,
        topic=event.topic,
        payload=UserJoinedECST.Payload(
            user_id=event.payload.user_id,  # type: ignore
            meeting_id=event.payload.meeting_id,  # type: ignore
        ),
    )
    context = serialization.SerializationContext(
        TOPIC_NAME,
        serialization.MessageField.VALUE,
    )

    return protobuf_serializer(proto_event, context)


async def main():
    event = cqrs.ECSTEvent[UserJoinedECSTPayload](
        event_name="user_joined_ecst",
        topic="user_joined_proto",
        payload=UserJoinedECSTPayload(user_id="123", meeting_id="456"),
    )
    kafka_producer = kafka_adapters.kafka_producer_factory(
        dsn="localhost:9092",
        value_serializer=serializer,
    )
    broker = kafka.KafkaMessageBroker(
        producer=kafka_producer,
    )
    event_producer = cqrs.EventProducer(message_broker=broker)
    await event_producer.send_message(session=None, event=event)


if __name__ == "__main__":
    asyncio.run(main())

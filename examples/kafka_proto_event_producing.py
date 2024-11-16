import asyncio
import typing

import pydantic
from confluent_kafka import schema_registry, serialization
from confluent_kafka.schema_registry import protobuf

import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka
from examples.proto.user_joined_pb2 import UserJoinedECST as UserJoinedECSTProtobuf  # type: ignore


class UserJoinedECSTPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str

    model_config = pydantic.ConfigDict(from_attributes=True)


class UserJoinedECST(cqrs.ECSTEvent[UserJoinedECSTPayload], frozen=True):
    def proto(self) -> UserJoinedECSTProtobuf:
        return UserJoinedECSTProtobuf(
            event_id=str(self.event_id),
            event_timestamp=str(self.event_timestamp),
            event_name=self.event_name,
            event_type=self.event_type,
            topic=self.topic,
            payload=UserJoinedECSTProtobuf.Payload(
                user_id=self.payload.user_id,  # type: ignore
                meeting_id=self.payload.meeting_id,  # type: ignore
            ),
        )


TOPIC_NAME = "user_joined_proto"
SUBJECT_NAME = f"{TOPIC_NAME}-value"
# Настройка Schema Registry
schema_registry_client = schema_registry.SchemaRegistryClient(
    {"url": "http://localhost:8085"},
)
protobuf_serializer = protobuf.ProtobufSerializer(
    UserJoinedECSTProtobuf,
    schema_registry_client,
    {"use.deprecated.format": False},
)


def serializer(event: cqrs.BaseNotificationEvent) -> typing.ByteString | None:
    context = serialization.SerializationContext(
        event.topic,
        serialization.MessageField.VALUE,
    )

    return protobuf_serializer(event.proto(), context)


async def main():
    event = UserJoinedECST(
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
    # More information about serialization:
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/protobuf_producer.py
    asyncio.run(main())

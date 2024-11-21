import asyncio

import pydantic

import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka
from cqrs.outbox import repository
from cqrs.serializers import protobuf
from examples.proto.user_joined_pb2 import UserJoinedECST as UserJoinedECSTProtobuf  # type: ignore


class UserJoinedECSTPayload(pydantic.BaseModel, frozen=True):
    user_id: str
    meeting_id: str

    model_config = pydantic.ConfigDict(from_attributes=True)


class UserJoinedECST(cqrs.NotificationEvent[UserJoinedECSTPayload], frozen=True):
    def proto(self) -> UserJoinedECSTProtobuf:
        return UserJoinedECSTProtobuf(
            event_id=str(self.event_id),
            event_timestamp=str(self.event_timestamp),
            event_name=self.event_name,
            payload=UserJoinedECSTProtobuf.Payload(
                user_id=self.payload.user_id,  # type: ignore
                meeting_id=self.payload.meeting_id,  # type: ignore
            ),
        )


async def main():
    event = UserJoinedECST(
        event_name="user_joined_ecst",
        topic="user_joined_proto",
        payload=UserJoinedECSTPayload(user_id="123", meeting_id="456"),
    )
    kafka_producer = kafka_adapters.kafka_producer_factory(
        dsn="localhost:9092",
        value_serializer=protobuf.protobuf_value_serializer,
    )
    broker = kafka.KafkaMessageBroker(
        producer=kafka_producer,
    )
    event_producer = cqrs.EventProducer(message_broker=broker)
    await event_producer.send_message(
        session=None,
        event=repository.OutboxedEvent(
            id=1,
            event=event,
            status=repository.EventStatus.NEW,
            topic=event.topic,
        ),
    )


if __name__ == "__main__":
    # More information about serialization:
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/protobuf_producer.py
    asyncio.run(main())

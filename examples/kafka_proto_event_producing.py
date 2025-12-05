"""
Example: Producing Protobuf Events to Kafka

This example demonstrates how to produce Protobuf-serialized events to Kafka.
The system shows how to use Protobuf for efficient binary serialization in
event-driven systems.

Use case: High-throughput event publishing with efficient serialization. Protobuf
provides compact binary format, faster serialization/deserialization, and schema
evolution support compared to JSON. This is ideal for systems publishing large
volumes of events.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Step 1: Start Kafka Infrastructure
-----------------------------------
   docker compose -f ./docker-compose-dev.yml up -d

Wait for Kafka to be ready (usually takes 30-60 seconds).

Step 2: Run the Producer
-------------------------
   python examples/kafka_proto_event_producing.py

The producer will:
- Create a UserJoinedECST event with Protobuf payload
- Convert the event to Protobuf format
- Publish the event to Kafka topic "user_joined_proto"
- Use Protobuf serialization for efficient binary encoding

Step 3: Verify Event (Optional)
---------------------------------
Run the consumer example to verify the event was published:
   python examples/kafka_proto_event_consuming.py

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Protobuf Event Definition:
   - Create NotificationEvent with typed payloads (Pydantic models)
   - Implement proto() method to convert events to Protobuf format
   - Map domain events to Protobuf schema

2. Protobuf Serialization:
   - Configure Kafka producer with protobuf_value_serializer
   - Serialize events to compact binary format
   - Reduce message size compared to JSON

3. Kafka Producer Configuration:
   - Set up Kafka producer with connection settings
   - Configure security protocols (PLAINTEXT or SASL_SSL)
   - Support for SSL/TLS and SASL authentication

4. Event Publishing:
   - Create OutboxedEvent wrapper for publishing
   - Send events to Kafka topics using message broker
   - Events are serialized and published asynchronously

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - pydantic (for typed payloads)
   - protobuf (Protobuf support)

Make sure Kafka is running:
   - Use docker-compose-dev.yml to start Kafka locally
   - Or configure connection to existing Kafka cluster

For more information about Protobuf serialization:
   https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/protobuf_producer.py

================================================================================
"""

import asyncio
import ssl

import pydantic

import cqrs
from cqrs.adapters import kafka as kafka_adapters
from cqrs.message_brokers import kafka, protocol as broker_protocol
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


def create_kafka_producer(
    ssl_context: ssl.SSLContext | None = None,
) -> kafka_adapters.KafkaProducer:
    dsn = "localhost:9092"
    value_serializer = protobuf.protobuf_value_serializer
    if ssl_context is None:
        return kafka_adapters.kafka_producer_factory(
            security_protocol="PLAINTEXT",
            sasl_mechanism="PLAIN",
            dsn=dsn,
            value_serializer=value_serializer,
        )
    return kafka_adapters.kafka_producer_factory(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        ssl_context=ssl_context,
        dsn=dsn,
        value_serializer=value_serializer,
    )


async def main():
    event = UserJoinedECST(
        event_name="user_joined_ecst",
        topic="user_joined_proto",
        payload=UserJoinedECSTPayload(user_id="123", meeting_id="456"),
    )
    kafka_producer = create_kafka_producer(ssl_context=None)
    broker = kafka.KafkaMessageBroker(
        producer=kafka_producer,
    )
    await broker.send_message(
        message=broker_protocol.Message(
            message_name=event.event_name,
            message_id=event.event_id,
            topic=event.topic,
            payload=repository.OutboxedEvent(
                id=1,
                event=event,
                status=repository.EventStatus.NEW,
                topic=event.topic,
            ),
        ),
    )


if __name__ == "__main__":
    # More information about serialization:
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/protobuf_producer.py
    asyncio.run(main())

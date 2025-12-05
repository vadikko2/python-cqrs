"""
Example: Consuming Protobuf Events from Kafka

This example demonstrates how to consume Protobuf-serialized events from Kafka
and process them using CQRS event handlers. The system shows how to use Protobuf
for efficient binary serialization in event-driven systems.

Use case: High-throughput event processing with efficient serialization. Protobuf
provides compact binary format, faster serialization/deserialization, and schema
evolution support compared to JSON. This is ideal for systems processing large
volumes of events.

================================================================================
HOW TO RUN THIS EXAMPLE
================================================================================

Step 1: Start Kafka Infrastructure
-----------------------------------
   docker compose -f ./docker-compose-dev.yml up -d

Wait for Kafka to be ready (usually takes 30-60 seconds).

Step 2: Send Protobuf Events to Kafka
--------------------------------------
In a separate terminal, run the producer:
   python examples/kafka_proto_event_producing.py

This will send a Protobuf-serialized UserJoinedECST event to the "user_joined_proto" topic.

Step 3: Run the Consumer
-------------------------
   python examples/kafka_proto_event_consuming.py

The consumer will:
- Connect to Kafka broker at localhost:9092
- Subscribe to "user_joined_proto" topic
- Deserialize Protobuf messages into UserJoinedECST events
- Process events through event handlers
- Print event details for each received event

================================================================================
WHAT THIS EXAMPLE DEMONSTRATES
================================================================================

1. Protobuf Deserialization:
   - Use ProtobufValueDeserializer to deserialize Kafka messages
   - Deserialize binary Protobuf data into typed event objects
   - Map Protobuf messages to domain event models

2. Protobuf Schema Integration:
   - Use generated Protobuf classes (UserJoinedECSTProtobuf)
   - Convert Protobuf messages to domain events
   - Handle schema evolution and versioning

3. Event Handler Processing:
   - Register event handlers for Protobuf events
   - EventMediator dispatches events to handlers
   - Handlers process events asynchronously

4. Error Handling:
   - Check for DeserializeProtobufError before processing
   - Acknowledge messages only after successful processing
   - Handle deserialization failures gracefully

================================================================================
REQUIREMENTS
================================================================================

Make sure you have installed:
   - cqrs (this package)
   - di (dependency injection)
   - faststream (Kafka integration)
   - protobuf (Protobuf support)

Make sure Kafka is running:
   - Use docker-compose-dev.yml to start Kafka locally
   - Or configure connection to existing Kafka cluster

For more information about Protobuf deserialization:
   https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/protobuf_consumer.py

================================================================================
"""

import asyncio
import logging

import cqrs
import di
import faststream
from cqrs.deserializers import protobuf
from cqrs.events import bootstrap
from faststream import kafka

from examples import kafka_proto_event_producing
from examples.proto.user_joined_pb2 import UserJoinedECST as UserJoinedECSTProtobuf  # type: ignore

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiokafka").setLevel(logging.ERROR)
logger = logging.getLogger("cqrs")

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])
app = faststream.FastStream(broker, logger=logger)

TOPIC_NAME = "user_joined_proto"


class UserJoinedECSTEventHandler(
    cqrs.EventHandler[kafka_proto_event_producing.UserJoinedECST],
):
    async def handle(
            self,
            event: kafka_proto_event_producing.UserJoinedECST,
    ) -> None:
        print(
            f"Handle user {event.payload.user_id} joined meeting {event.payload.meeting_id} event",
        )


def events_mapper(mapper: cqrs.EventMap) -> None:
    """Maps events to handlers."""
    mapper.bind(
        kafka_proto_event_producing.UserJoinedECST,
        UserJoinedECSTEventHandler,
    )


def mediator_factory() -> cqrs.EventMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )


@broker.subscriber(
    TOPIC_NAME,
    group_id="protobuf_consumers",
    auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=protobuf.ProtobufValueDeserializer(
        model=kafka_proto_event_producing.UserJoinedECST,
        protobuf_model=UserJoinedECSTProtobuf,
    ),
)
async def consumer(
        body: kafka_proto_event_producing.UserJoinedECST | protobuf.DeserializeProtobufError,
        msg: kafka.KafkaMessage,
        mediator: cqrs.EventMediator = faststream.Depends(mediator_factory),
) -> None:
    if not isinstance(body, protobuf.DeserializeProtobufError):
        await mediator.send(body)
    await msg.ack()


if __name__ == "__main__":
    # More information about deserialization:
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/protobuf_consumer.py
    print(
        "1. Run kafka infrastructure with: `docker compose -f ./docker-compose-dev.yml up -d`\n"
        "2. Send event to kafka topic via `python examples/kafka_proto_event_producing.py`",
    )
    asyncio.run(app.run())

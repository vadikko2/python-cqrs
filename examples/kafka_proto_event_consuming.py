import asyncio
import logging

import di
import faststream
from faststream import kafka

import cqrs
from cqrs.deserializers import protobuf
from cqrs.events import bootstrap
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
    body: kafka_proto_event_producing.UserJoinedECST | None,
    msg: kafka.KafkaMessage,
    mediator: cqrs.EventMediator = faststream.Depends(mediator_factory),
) -> None:
    if body is not None:
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

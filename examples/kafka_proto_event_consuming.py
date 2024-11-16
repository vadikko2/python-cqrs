import asyncio
import logging
import typing

import di
import faststream
from confluent_kafka.schema_registry import protobuf
from faststream import kafka

import cqrs
from cqrs.events import bootstrap
from examples import kafka_proto_event_producing
from examples.proto.user_joined_pb2 import UserJoinedECST as UserJoinedECSTProtobuf  # type: ignore

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiokafka").setLevel(logging.ERROR)

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])
app = faststream.FastStream(broker)

TOPIC_NAME = "user_joined_proto"

EVENT_REGISTRY = dict(
    user_joined_ecst=kafka_proto_event_producing.UserJoinedECST,
)


class UserJoinedECSTEventHandler(
    cqrs.EventHandler[cqrs.ECSTEvent[kafka_proto_event_producing.UserJoinedECST]],
):
    async def handle(
        self,
        event: cqrs.ECSTEvent[kafka_proto_event_producing.UserJoinedECST],
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


def common_deserializer(
    msg: typing.ByteString,
) -> kafka_proto_event_producing.UserJoinedECST | None:
    """
    Deserialize protobuf message into CQRS event model.
    """
    protobuf_deserializer = protobuf.ProtobufDeserializer(
        UserJoinedECSTProtobuf,
        {"use.deprecated.format": False},
    )
    proto_event = protobuf_deserializer(msg, None)
    if proto_event is None:
        return
    model = EVENT_REGISTRY.get(proto_event.event_name)  # type: ignore
    if model is None:
        return

    return model.model_validate(proto_event)


@broker.subscriber(
    TOPIC_NAME,
    group_id="protobuf_consumers",
    auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=common_deserializer,
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

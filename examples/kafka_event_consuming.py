import asyncio
import logging

import di
import faststream
import orjson
import pydantic
from faststream import kafka

import cqrs
from cqrs import deserializers, decoders
from cqrs.events import bootstrap

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiokafka").setLevel(logging.ERROR)

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])
app = faststream.FastStream(broker)


class HelloWorldPayload(pydantic.BaseModel):
    hello: str = pydantic.Field(default="Hello")
    world: str = pydantic.Field(default="World")


class HelloWorldECSTEventHandler(
    cqrs.EventHandler[cqrs.NotificationEvent[HelloWorldPayload]],
):
    async def handle(self, event: cqrs.NotificationEvent[HelloWorldPayload]) -> None:
        print(f"{event.payload.hello} {event.payload.world}")  # type: ignore


def events_mapper(mapper: cqrs.EventMap) -> None:
    """Maps events to handlers."""
    mapper.bind(cqrs.NotificationEvent[HelloWorldPayload], HelloWorldECSTEventHandler)


def mediator_factory() -> cqrs.EventMediator:
    return bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )


@broker.subscriber(
    "hello_world",
    group_id="examples",
    auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=deserializers.JsonDeserializer(
        model=cqrs.NotificationEvent[HelloWorldPayload],
    ),
    decoder=decoders.empty_message_decoder,
)
async def hello_world_event_handler(
    body: cqrs.NotificationEvent[HelloWorldPayload] | None,
    msg: kafka.KafkaMessage,
    mediator: cqrs.EventMediator = faststream.Depends(mediator_factory),
):
    if body is not None:
        await mediator.send(body)
    await msg.ack()


if __name__ == "__main__":
    ev = cqrs.NotificationEvent[HelloWorldPayload](
        event_name="HelloWorldECSTEvent",
        topic="hello_world",
        payload=HelloWorldPayload(),
    )
    print(
        f"1. Run kafka infrastructure with: `docker compose -f ./docker-compose-dev.yml up -d`\n"
        f"2. Send to kafka topic `hello_world` event: {orjson.dumps(ev.model_dump(mode='json')).decode()}",
    )
    asyncio.run(app.run())

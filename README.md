# Python CQRS pattern implementation with Transaction Outbox supporting

## Overview

This is a package for implementing the CQRS (Command Query Responsibility Segregation) pattern in Python applications.
It provides a set of abstractions and utilities to help separate read and write use cases, ensuring better scalability,
performance, and maintainability of the application.

This package is a fork of the [diator](https://github.com/akhundMurad/diator)
project ([documentation](https://akhundmurad.github.io/diator/)) with several enhancements:

1. Support for Pydantic [v2.*](https://docs.pydantic.dev/2.8/);
2. `Kafka` support using [aiokafka](https://github.com/aio-libs/aiokafka);
3. Added `EventMediator` for handling `Notification` and `ECST` events coming from the bus;
4. Redesigned the event and request mapping mechanism to handlers;
5. Added `bootstrap` for easy setup;
6. Added support for [Transaction Outbox](https://microservices.io/patterns/data/transactional-outbox.html), ensuring
   that `Notification` and `ECST` events are sent to the broker;
7. FastAPI supporting;
8. FastStream supporting.

## Request Handlers

Request handlers can be divided into two main types:

### Command Handler

Command Handler executes the received command. The logic of the handler may include, for example, modifying the state of
the domain model.
As a result of executing the command, an event may be produced to the broker.
> [!TIP]
> By default, the command handler does not return any result, but it is not mandatory.

```python
from cqrs.requests.request_handler import RequestHandler, SyncRequestHandler
from cqrs.events.event import Event

class JoinMeetingCommandHandler(RequestHandler[JoinMeetingCommand, None]):

      def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
          self._meetings_api = meetings_api
          self.events: list[Event] = []

      @property
      def events(self) -> typing.List[events.Event]:
          return self._events

      async def handle(self, request: JoinMeetingCommand) -> None:
          await self._meetings_api.join_user(request.user_id, request.meeting_id)


class SyncJoinMeetingCommandHandler(SyncRequestHandler[JoinMeetingCommand, None]):

      def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
          self._meetings_api = meetings_api
          self.events: list[Event] = []

      @property
      def events(self) -> typing.List[events.Event]:
          return self._events

      def handle(self, request: JoinMeetingCommand) -> None:
          # do some sync logic
          ...
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/request_handler.py)

### Query handler

Query Handler returns a representation of the requested data, for example, from
the [read model](https://radekmaziarka.pl/2018/01/08/cqrs-third-step-simple-read-model/#simple-read-model---to-the-rescue).
> [!TIP]
> The read model can be constructed based on domain events produced by the `Command Handler`.

```python
from cqrs.requests.request_handler import RequestHandler
from cqrs.events.event import Event

class ReadMeetingQueryHandler(RequestHandler[ReadMeetingQuery, ReadMeetingQueryResult]):

      def __init__(self, meetings_api: MeetingAPIProtocol) -> None:
          self._meetings_api = meetings_api
          self.events: list[Event] = []

      @property
      def events(self) -> typing.List[events.Event]:
          return self._events

      async def handle(self, request: ReadMeetingQuery) -> ReadMeetingQueryResult:
          link = await self._meetings_api.get_link(request.meeting_id)
          return ReadMeetingQueryResult(link=link, meeting_id=request.meeting_id)

```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/request_handler.py)

## Event Handlers

Event handlers are designed to process `Notification` and `ECST` events that are consumed from the broker.
To configure event handling, you need to implement a broker consumer on the side of your application.
Below is an example of `Kafka event consuming` that can be used in the Presentation Layer.

```python
class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        STORAGE[request.meeting_id].append(request.user_id)
        self._events.append(
            UserJoined(user_id=request.user_id, meeting_id=request.meeting_id),
        )
        print(f"User {request.user_id} joined meeting {request.meeting_id}")


class UserJoinedEventHandler(cqrs.EventHandler[UserJoined]):
    async def handle(self, event: UserJoined) -> None:
        print(f"Handle user {event.user_id} joined meeting {event.meeting_id} event")
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/domain_event_handler.py)

## Producing Notification/ECST Events

During the handling of a command event, messages of type `cqrs.NotificationEvent` or `cqrs.ECSTEvent` may be generated
and then sent to the broker.

```python
class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        self._events.append(
            cqrs.NotificationEvent[UserJoinedNotificationPayload](
                event_name="UserJoined",
                topic="user_notification_events",
                payload=UserJoinedNotificationPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            )
        )
        self._events.append(
            cqrs.ECSTEvent[UserJoinedECSTPayload](
                event_name="UserJoined",
                topic="user_ecst_events",
                payload=UserJoinedECSTPayload(
                    user_id=request.user_id,
                    meeting_id=request.meeting_id,
                ),
            )
        )
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/event_producing.py)

After processing the command/request, if there are any Notification/ECST events,
the EventEmitter is invoked to produce the events via the message broker.

> [!WARNING]
> It is important to note that producing events using the events property parameter does not guarantee message delivery
> to the broker.
> In the event of broker unavailability or an exception occurring during message formation or sending, the message may
> be lost.
> This issue can potentially be addressed by configuring retry attempts for sending messages to the broker, but we
> recommend using the [Transaction Outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern,
> which is implemented in the current version of the python-cqrs package for this purpose.

## Kafka broker

```python
from cqrs.adapters import kafka as kafka_adapter
from cqrs.message_brokers import kafka as kafka_broker


producer = kafka_adapter.kafka_producer_factory(
    dsn="localhost:9094",
    topics=["test.topic1", "test.topic2"],
)
broker = kafka_broker.KafkaMessageBroker(producer)
await broker.send_message(...)
```

## Transactional Outbox

The package implements the [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
pattern, which ensures that messages are produced to the broker according to the at-least-once semantics.

```python
def do_some_logic(meeting_room_id: int, session: sql_session.AsyncSession):
    """
    Make changes to the database
    """
    session.add(...)


class JoinMeetingCommandHandler(cqrs.RequestHandler[JoinMeetingCommand, None]):
    def __init__(self, outbox: cqrs.OutboxedEventRepository):
        self.outbox = outbox

    @property
    def events(self):
        return []

    async def handle(self, request: JoinMeetingCommand) -> None:
        print(f"User {request.user_id} joined meeting {request.meeting_id}")
        async with self.outbox as session:
            do_some_logic(request.meeting_id, session) # business logic
            self.outbox.add(
                session,
                cqrs.NotificationEvent[UserJoinedNotificationPayload](
                    event_name="UserJoined",
                    topic="user_notification_events",
                    payload=UserJoinedNotificationPayload(
                        user_id=request.user_id,
                        meeting_id=request.meeting_id,
                    ),
                ),
            )
            self.outbox.add(
                session,
                cqrs.ECSTEvent[UserJoinedECSTPayload](
                    event_name="UserJoined",
                    topic="user_ecst_events",
                    payload=UserJoinedECSTPayload(
                        user_id=request.user_id,
                        meeting_id=request.meeting_id,
                    ),
                ),
            )
            await self.outbox.commit(session)
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/save_events_into_outbox.py)

> [!TIP]
> You can specify the name of the Outbox table using the environment variable `OUTBOX_SQLA_TABLE`.
> By default, it is set to `outbox`.

## Producing Events from Outbox to Kafka

As an implementation of the Transactional Outbox pattern, the SqlAlchemyOutboxedEventRepository is available for use as
an access repository to the Outbox storage.
It can be utilized in conjunction with the KafkaMessageBroker.

```python
import asyncio
import cqrs
from cqrs.message_brokers import kafka as kafka_broker

session_factory = async_sessionmaker(
    create_async_engine(
        f"mysql+asyncmy://{USER}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}",
        isolation_level="REPEATABLE READ",
    )
)

broker = kafka.KafkaMessageBroker(
  producer=kafka_adapters.kafka_producer_factory(dsn="localhost:9092"),
)

producer = cqrs.EventProducer(cqrs.SqlAlchemyOutboxedEventRepository(session_factory, zlib.ZlibCompressor()), broker)
loop = asyncio.get_event_loop()
loop.run_until_complete(app.periodically_task())
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/kafka_outboxed_event_producing.py)

## Transaction log tailing

If the Outbox polling strategy does not suit your needs, I recommend exploring
the [Transaction Log Tailing](https://microservices.io/patterns/data/transaction-log-tailing.html) pattern.
The current version of the python-cqrs package does not support the implementation of this pattern.

> [!TIP]
> However, it can be implemented
> using [Debezium + Kafka Connect](https://debezium.io/documentation/reference/stable/architecture.html),
> which allows you to produce all newly created events within the Outbox storage directly to the corresponding topic in
> Kafka (or any other broker).

## DI container

Use the following example to set up dependency injection in your command, query and event handlers. This will make
dependency management simpler.

```python
import di
...

def setup_di() -> di.Container:
    """
    Binds implementations to dependencies
    """
    container = di.Container()
    container.bind(
        di.bind_by_type(
            dependent.Dependent(cqrs.SqlAlchemyOutboxedEventRepository, scope="request"),
            cqrs.OutboxedEventRepository
        )
    )
    container.bind(
        di.bind_by_type(
            dependent.Dependent(MeetingAPIImplementaion, scope="request"),
            MeetingAPIProtocol
        )
    )
    return container
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/dependency_injection.py)

## Mapping

To bind commands, queries and events with specific handlers, you can use the registries `EventMap` and `RequestMap`.

```python
from cqrs import requests, events

from app import commands, command_handlers
from app import queries, query_handlers
from app import events as event_models, event_handlers


def init_commands(mapper: requests.RequestMap) -> None:
    mapper.bind(commands.JoinMeetingCommand, command_handlers.JoinMeetingCommandHandler)

def init_queries(mapper: requests.RequestMap) -> None:
    mapper.bind(queries.ReadMeetingQuery, query_handlers.ReadMeetingQueryHandler)

def init_events(mapper: events.EventMap) -> None:
    mapper.bind(events.NotificationEvent[events_models.NotificationMeetingRoomClosed], event_handlers.MeetingRoomClosedNotificationHandler)
    mapper.bind(events.ECSTEvent[event_models.ECSTMeetingRoomClosed], event_handlers.UpdateMeetingRoomReadModelHandler)
```

## Bootstrap

The `python-cqrs` package implements a set of bootstrap utilities designed to simplify the initial configuration of an
application.

```python
import functools

from cqrs.events import bootstrap as event_bootstrap
from cqrs.requests import bootstrap as request_bootstrap

from app import dependencies, mapping, orm


@functools.lru_cache
def mediator_factory():
    return request_bootstrap.bootstrap(
        di_container=dependencies.setup_di(),
        commands_mapper=mapping.init_commands,
        queries_mapper=mapping.init_queries,
        domain_events_mapper=mapping.init_events,
        on_startup=[orm.init_store_event_mapper],
    )


@functools.lru_cache
def event_mediator_factory():
    return event_bootstrap.bootstrap(
        di_container=dependencies.setup_di(),
        events_mapper=mapping.init_events,
        on_startup=[orm.init_store_event_mapper],
    )
```

## Integration with presentation layers

> [!TIP]
> I recommend reading the useful
>
paper [Onion Architecture Used in Software Development](https://www.researchgate.net/publication/371006360_Onion_Architecture_Used_in_Software_Development).
> Separating user interaction and use-cases into Application and Presentation layers is a good practice.
> This can improve the `Testability`, `Maintainability`, `Scalability` of the application. It also provides benefits
> such as `Separation of Concerns`.

### FastAPI requests handling

If your application uses FastAPI (or any other asynchronous framework for creating APIs).
In this case you can use python-cqrs to route requests to the appropriate handlers implementing specific use-cases.

```python
import fastapi
import pydantic

from app import dependecies, commands

router = fastapi.APIRouter(prefix="/meetings")


@router.put("/{meeting_id}/{user_id}", status_code=status.HTTP_200_OK)
async def join_metting(
    meeting_id: pydantic.PositiveInt,
    user_id: typing.Text,
    mediator: cqrs.RequestMediator = fastapi.Depends(dependencies.mediator_factory),
):
    await mediator.send(commands.JoinMeetingCommand(meeting_id=meeting_id, user_id=user_id))
    return {"result": "ok"}
```

A complete example can be found in
the [documentation](https://github.com/vadikko2/cqrs/blob/master/examples/fastapi_integration.py)

### Kafka events consuming

If you build interaction by events over broker like `Kafka`, you can to implement an event consumer on your
application's side,
which will call the appropriate handler for each event.
An example of handling events from `Kafka` is provided below.

```python
import cqrs

import pydantic
import faststream
from faststream import kafka

broker = kafka.KafkaBroker(bootstrap_servers=["localhost:9092"])
app = faststream.FastStream(broker)


class HelloWorldPayload(pydantic.BaseModel):
    hello: str = pydantic.Field(default="Hello")
    world: str = pydantic.Field(default="World")


class HelloWorldECSTEventHandler(cqrs.EventHandler[cqrs.ECSTEvent[HelloWorldPayload]]):
    async def handle(self, event: cqrs.ECSTEvent[HelloWorldPayload]) -> None:
        print(f"{event.payload.hello} {event.payload.world}")  # type: ignore


@broker.subscriber(
    "hello_world",
    group_id="examples",
    auto_commit=False,
    value_deserializer=value_deserializer,
)
async def hello_world_event_handler(
    body: cqrs.ECSTEvent[HelloWorldPayload] | None,
    msg: kafka.KafkaMessage,
    mediator: cqrs.EventMediator = faststream.Depends(mediator_factory),
):
    if body is not None:
        await mediator.send(body)
    await msg.ack()
```

A complete example can be found in the [documentation](https://github.com/vadikko2/python-cqrs/blob/master/examples/kafka_event_consuming.py)

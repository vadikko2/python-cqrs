import contextlib
import logging
import typing
import uuid

from sqlalchemy.ext.asyncio import session as sql_session

from cqrs.message_brokers import kafka
from cqrs.message_brokers import protocol as broker_protocol
from cqrs.outbox import protocol, sqlalchemy

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)

OutboxFactory = typing.Callable[[], protocol.Outbox]
SessionFactory: typing.TypeAlias = typing.Callable[[], sql_session.AsyncSession]


class SQlAlchemyKafkaEventProducer(protocol.EventProducer):
    def __init__(
        self,
        sqla_session_factory: SessionFactory,
        message_broker: kafka.KafkaMessageBroker,
    ):
        self.sqla_session_factory = sqla_session_factory
        self.message_broker = message_broker

    @contextlib.asynccontextmanager
    async def transaction(self) -> sql_session.AsyncSession:
        async with contextlib.aclosing(self.sqla_session_factory()) as session:
            try:
                yield session
            finally:
                await session.rollback()

    async def produce_one(self, event_id: uuid.UUID) -> None:
        async with self.transaction() as session:
            outbox = sqlalchemy.SqlAlchemyOutbox(session)
            event: protocol.Event | None = await outbox.get_event(event_id)
            if event:
                try:
                    await self.message_broker.send_message(
                        broker_protocol.Message(
                            message_type=event.event_type,
                            message_name=event.event_name,
                            message_id=event.event_id,
                            payload=event.model_dump(mode="json"),
                        ),
                    )
                except Exception as e:
                    logger.error(f"Error while producing event {event_id} to kafka broker: {e}")
                    await outbox.mark_as_failure(event_id)
                else:
                    await outbox.mark_as_produced(event_id)
            await outbox.save()

    async def produce_batch(self, batch_size: int = 100) -> None:
        async with self.transaction() as session:
            outbox = sqlalchemy.SqlAlchemyOutbox(session)
            events = await outbox.get_events(batch_size)
            for event in events:
                try:
                    await self.message_broker.send_message(
                        broker_protocol.Message(
                            message_type=event.event_type,
                            message_name=event.event_name,
                            message_id=event.event_id,
                            payload=event.model_dump(mode="json"),
                        ),
                    )
                except Exception as e:
                    logger.error(f"Error while producing event {event.event_id} to kafka broker: {e}")
                    await outbox.mark_as_failure(event.event_id)
                else:
                    await outbox.mark_as_produced(event.event_id)
            await outbox.save()

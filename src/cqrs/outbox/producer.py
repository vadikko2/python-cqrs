import asyncio
import logging
import typing
import uuid

from sqlalchemy.ext.asyncio import session as sql_session

import cqrs
from cqrs.message_brokers import protocol as broker_protocol
from cqrs.outbox import repository as repository_protocol

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)

SessionFactory: typing.TypeAlias = typing.Callable[[], sql_session.AsyncSession]


class EventProducer:
    def __init__(
        self,
        message_broker: broker_protocol.MessageBroker,
        repository: repository_protocol.OutboxedEventRepository | None = None,
    ):
        self.message_broker = message_broker
        self.repository = repository

    async def periodically_task(
        self,
        batch_size: int = 100,
        wait_ms: int = 500,
    ) -> None:
        """Calls produce periodically with specified delay"""
        while True:
            await asyncio.sleep(float(wait_ms) / 1000.0)
            await self.produce_batch(batch_size)

    async def send_message(
        self,
        session: object,
        event: cqrs.BaseNotificationEvent,
    ):
        try:
            logger.debug(f"Send event {event.event_id} into topic {event.topic}")
            await self.message_broker.send_message(
                broker_protocol.Message(
                    message_type=event.event_type,
                    message_name=event.event_name,
                    message_id=event.event_id,
                    topic=event.topic,
                    payload=event,
                ),
            )
        except Exception as error:
            logger.error(
                f"Error while producing event {event.event_id} to kafka broker: {error}",
            )
            if not self.repository:
                return
            await self.repository.update_status(
                session,
                event.event_id,
                repository_protocol.EventStatus.NOT_PRODUCED,
            )
        else:
            if not self.repository:
                return
            await self.repository.update_status(
                session,
                event.event_id,
                repository_protocol.EventStatus.PRODUCED,
            )

    async def produce_one(self, event_id: uuid.UUID) -> None:
        if not self.repository:
            logger.debug("Repository not found")
            return
        async with self.repository as session:
            event = await self.repository.get_one(session, event_id)
            if event:
                await self.send_message(session, event)
            await self.repository.commit(session)

    async def produce_batch(self, batch_size: int = 100) -> None:
        if not self.repository:
            logger.debug("Repository not found")
            return
        async with self.repository as session:
            events = await self.repository.get_many(session, batch_size)
            logger.debug(f"Got {len(events)} new events")
            for event in events:
                await self.send_message(session, event)
            await self.repository.commit(session)

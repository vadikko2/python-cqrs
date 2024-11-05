import asyncio
import enum
import logging
import os
import typing
import uuid

import dotenv
import orjson
import sqlalchemy
from sqlalchemy import func
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.asyncio import session as sql_session
from sqlalchemy.orm import registry

from cqrs.events import event as ev
from cqrs.outbox import repository

mapper_registry = registry()
Base = mapper_registry.generate_base()

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

OUTBOX_TABLE = os.getenv("OUTBOX_SQLA_TABLE", "outbox")


class EventType(enum.StrEnum):
    ECST_EVENT = "ecst_event"
    NOTIFICATION_EVENT = "notification_event"


MAX_FLUSH_COUNTER_VALUE = 5


class OutboxModel(Base):
    __tablename__ = OUTBOX_TABLE

    __table_args__ = (
        sqlalchemy.UniqueConstraint(
            "event_id_bin",
            "event_name",
            name="event_id_unique_index",
        ),
    )
    id = sqlalchemy.Column(
        sqlalchemy.BigInteger(),
        sqlalchemy.Identity(),
        primary_key=True,
        nullable=False,
        autoincrement=True,
        comment="Identity",
    )
    event_id = sqlalchemy.Column(
        sqlalchemy.Uuid,
        nullable=False,
        comment="Event idempotency id",
    )
    event_id_bin = sqlalchemy.Column(
        sqlalchemy.BINARY(16),
        nullable=False,
        comment="Event idempotency id in 16 bit presentation",
    )
    event_type = sqlalchemy.Column(
        sqlalchemy.Enum(EventType),
        nullable=False,
        comment="Event type",
    )
    event_status = sqlalchemy.Column(
        sqlalchemy.Enum(repository.EventStatus),
        nullable=False,
        default=repository.EventStatus.NEW,
        comment="Event producing status",
    )
    flush_counter = sqlalchemy.Column(
        sqlalchemy.SmallInteger(),
        nullable=False,
        default=0,
        comment="Event producing flush counter",
    )
    event_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Event name",
    )
    topic = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Event topic",
        default="",
    )
    created_at = sqlalchemy.Column(
        sqlalchemy.DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Event creation timestamp",
    )
    payload = sqlalchemy.Column(
        mysql.BLOB,
        nullable=False,
        default={},
        comment="Event payload",
    )

    def row_to_dict(self) -> typing.Dict[typing.Text, typing.Any]:
        return {
            column.name: getattr(self, column.name) for column in self.__table__.columns
        }

    @classmethod
    def get_batch_query(
        cls,
        size: int,
        topic: typing.Text | None = None,
    ) -> sqlalchemy.Select:
        return (
            sqlalchemy.select(cls)
            .select_from(cls)
            .where(
                sqlalchemy.and_(
                    cls.event_status.in_(
                        [
                            repository.EventStatus.NEW,
                            repository.EventStatus.NOT_PRODUCED,
                        ],
                    ),
                    cls.flush_counter < MAX_FLUSH_COUNTER_VALUE,
                    (cls.topic == topic) if topic is not None else sqlalchemy.true(),
                ),
            )
            .order_by(cls.status_sorting_case().asc())
            .order_by(cls.id.asc())
            .limit(size)
        )

    @classmethod
    def get_event_query(cls, event_id: uuid.UUID) -> sqlalchemy.Select:
        return (
            sqlalchemy.select(cls)
            .select_from(cls)
            .where(
                sqlalchemy.and_(
                    cls.event_id_bin == func.UUID_TO_BIN(event_id),
                    cls.event_status.in_(
                        [
                            repository.EventStatus.NEW,
                            repository.EventStatus.NOT_PRODUCED,
                        ],
                    ),
                    cls.flush_counter < MAX_FLUSH_COUNTER_VALUE,
                ),
            )
            .order_by(cls.status_sorting_case().asc())
            .order_by(cls.id.asc())
        )

    @classmethod
    def update_status_query(
        cls,
        event_id: uuid.UUID,
        status: repository.EventStatus,
    ) -> sqlalchemy.Update:
        values = {
            "event_status": status,
            "flush_counter": cls.flush_counter,
        }
        if status == repository.EventStatus.NOT_PRODUCED:
            values["flush_counter"] += 1

        return (
            sqlalchemy.update(cls)
            .where(cls.event_id_bin == func.UUID_TO_BIN(event_id))
            .values(**values)
        )

    @classmethod
    def status_sorting_case(cls) -> sqlalchemy.Case:
        return sqlalchemy.case(
            {
                repository.EventStatus.NEW: 1,
                repository.EventStatus.NOT_PRODUCED: 2,
                repository.EventStatus.PRODUCED: 3,
            },
            value=cls.event_status,
            else_=4,
        )


class SqlAlchemyOutboxedEventRepository(
    repository.OutboxedEventRepository[sql_session.AsyncSession],
):
    EVENT_CLASS_MAPPING: typing.ClassVar[
        typing.Dict[
            EventType,
            typing.Type[ev.NotificationEvent] | typing.Type[ev.ECSTEvent],
        ]
    ] = {
        EventType.NOTIFICATION_EVENT: ev.NotificationEvent,
        EventType.ECST_EVENT: ev.ECSTEvent,
    }

    def add(
        self,
        session: sql_session.AsyncSession,
        event,
    ) -> None:
        bytes_payload = orjson.dumps(event.payload)
        if self._compressor:
            bytes_payload = self._compressor.compress(bytes_payload)
        session.add(
            OutboxModel(
                event_id=event.event_id,
                event_id_bin=func.UUID_TO_BIN(event.event_id),
                event_type=EventType(event.event_type),
                event_name=event.event_name,
                created_at=event.event_timestamp,
                payload=bytes_payload,
                topic=event.topic,
            ),
        )

    def _process_events(self, model: OutboxModel) -> repository.Event:
        event_dict = model.row_to_dict()
        if event_dict["payload"] is not None:
            event_dict["payload"] = orjson.loads(
                self._compressor.decompress(event_dict["payload"])
                if self._compressor
                else event_dict["payload"],
            )
        return self.EVENT_CLASS_MAPPING[event_dict["event_type"]].model_validate(
            event_dict,
        )  # type: ignore

    async def get_many(
        self,
        session: sql_session.AsyncSession,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.Event]:
        events: typing.Sequence[OutboxModel] = (
            (await session.execute(OutboxModel.get_batch_query(batch_size, topic)))
            .scalars()
            .all()
        )

        tasks = []
        for event in events:
            if not self.EVENT_CLASS_MAPPING.get(EventType(event.event_type)):
                logger.warning(f"Unknown event type for {event}")
                continue
            tasks.append(asyncio.to_thread(self._process_events, event))

        return await asyncio.gather(*tasks)  # noqa

    async def get_one(
        self,
        session: sql_session.AsyncSession,
        event_id,
    ) -> repository.Event | None:
        event: OutboxModel | None = (
            await session.execute(OutboxModel.get_event_query(event_id))
        ).scalar()

        if event is None:
            return

        if not self.EVENT_CLASS_MAPPING.get(event.event_type):
            logger.warning(f"Unknown event type for {event}")
            return

        event_dict = event.row_to_dict()
        event_dict["payload"] = orjson.loads(
            self._compressor.decompress(event_dict["payload"])
            if self._compressor
            else event_dict["payload"],
        )

        return self._process_events(event)

    async def update_status(
        self,
        session: sql_session.AsyncSession,
        event_id: uuid.UUID,
        new_status: repository.EventStatus,
    ) -> None:
        await session.execute(
            statement=OutboxModel.update_status_query(event_id, new_status),
        )

    async def commit(self, session: sql_session.AsyncSession):
        await session.commit()

    async def rollback(self, session: sql_session.AsyncSession):
        await session.rollback()

    async def __aenter__(self):
        self.session = self._session_factory()
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.rollback()
            await self.session.close()
            self.session = None

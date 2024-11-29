import logging
import typing

import dotenv
import orjson
import sqlalchemy
from sqlalchemy import func
from sqlalchemy.dialects import mysql
from sqlalchemy.ext.asyncio import session as sql_session
from sqlalchemy.orm import DeclarativeMeta, registry

import cqrs
from cqrs import compressors
from cqrs.outbox import map, repository

Base = registry().generate_base()

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

DEFAULT_OUTBOX_TABLE_NAME = "outbox"

MAX_FLUSH_COUNTER_VALUE = 5


class OutboxModel(Base):
    __tablename__ = DEFAULT_OUTBOX_TABLE_NAME

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
    def update_status_query(
        cls,
        outboxed_event_id: int,
        status: repository.EventStatus,
    ) -> sqlalchemy.Update:
        values = {
            "event_status": status,
            "flush_counter": cls.flush_counter,
        }
        if status == repository.EventStatus.NOT_PRODUCED:
            values["flush_counter"] += 1

        return (
            sqlalchemy.update(cls).where(cls.id == outboxed_event_id).values(**values)
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
    def __init__(
        self,
        session: sql_session.AsyncSession,
        compressor: compressors.Compressor | None = None,
    ):
        self.session = session
        self._compressor = compressor

    def add(
        self,
        event: cqrs.NotificationEvent,
    ) -> None:
        registered_event = map.OutboxedEventMap.get(event.event_name)
        if registered_event is None:
            raise TypeError(f"Unknown event name for {event.event_name}")

        if type(event) is not registered_event:
            raise TypeError(
                f"Event type {type(event)} does not match registered event type {registered_event}",
            )

        bytes_payload = orjson.dumps(event.model_dump(mode="json"))
        if self._compressor is not None:
            bytes_payload = self._compressor.compress(bytes_payload)

        self.session.add(
            OutboxModel(
                event_id=event.event_id,
                event_id_bin=func.UUID_TO_BIN(event.event_id),
                event_name=event.event_name,
                created_at=event.event_timestamp,
                payload=bytes_payload,
                topic=event.topic,
            ),
        )

    def _process_events(self, model: OutboxModel) -> repository.OutboxedEvent | None:
        event_dict = model.row_to_dict()

        event_model = map.OutboxedEventMap.get(event_dict["event_name"])
        if event_model is None:
            return

        if self._compressor is not None:
            event_dict["payload"] = self._compressor.decompress(event_dict["payload"])
        event_dict["payload"] = orjson.loads(event_dict["payload"])

        return repository.OutboxedEvent(
            id=event_dict["id"],
            topic=event_dict["topic"],
            status=event_dict["event_status"],
            event=event_model.model_validate(event_dict["payload"]),
        )

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.OutboxedEvent]:
        events: typing.Sequence[OutboxModel] = (
            (await self.session.execute(OutboxModel.get_batch_query(batch_size, topic)))
            .scalars()
            .all()
        )

        result = []
        for event in events:
            outboxed_event = self._process_events(event)
            if outboxed_event is None:
                logger.warning(f"Unknown event name for {event.event_name}")
                continue
            result.append(outboxed_event)

        return result

    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ) -> None:
        await self.session.execute(
            statement=OutboxModel.update_status_query(outboxed_event_id, new_status),
        )

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()


def rebind_outbox_model(
    model: typing.Any,
    new_base: DeclarativeMeta,
    table_name: typing.Text | None = None,
):
    model.__bases__ = (new_base,)
    model.__table__.name = table_name or model.__table__.name
    new_base.metadata._add_table(
        model.__table__.name,
        model.__table__.schema,
        model.__table__,
    )

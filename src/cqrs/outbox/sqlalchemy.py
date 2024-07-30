import enum
import logging
import typing
import uuid

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.ext.asyncio import session as sql_session
from sqlalchemy.orm import registry

from cqrs.events import event as ev
from cqrs.outbox import protocol

mapper_registry = registry()
Base = mapper_registry.generate_base()

logger = logging.getLogger(__name__)


class EventType(enum.StrEnum):
    ECST_EVENT = "ecst_event"
    NOTIFICATION_EVENT = "notification_event"


class EventStatus(enum.StrEnum):
    NEW = "new"
    PRODUCED = "produced"
    NOT_PRODUCED = "not_produced"


MAX_FLUSH_COUNTER_VALUE = 5


class OutboxModel(Base):
    __tablename__ = "outbox"

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
    event_id = sqlalchemy.Column(sqlalchemy.Uuid, nullable=False, comment="Event idempotency id")
    event_id_bin = sqlalchemy.Column(
        sqlalchemy.BINARY(16),
        nullable=False,
        comment="Event idempotency id in 16 bit presentation",
    )
    event_type = sqlalchemy.Column(sqlalchemy.Enum(EventType), nullable=False, comment="Event type")
    event_status = sqlalchemy.Column(
        sqlalchemy.Enum(EventStatus),
        nullable=False,
        default=EventStatus.NEW,
        comment="Event producing status",
    )
    flush_counter = sqlalchemy.Column(
        sqlalchemy.SmallInteger(),
        nullable=False,
        default=0,
        comment="Event producing flush counter",
    )
    event_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False, comment="Event name")

    created_at = sqlalchemy.Column(
        sqlalchemy.DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Event creation timestamp",
    )
    payload = sqlalchemy.Column(sqlalchemy.JSON, nullable=False, default={}, comment="Event payload")

    @classmethod
    def get_batch_query(cls, size: int) -> sqlalchemy.Select:
        return (
            sqlalchemy.select(cls)
            .select_from(cls)
            .where(
                sqlalchemy.and_(
                    cls.event_status.in_([EventStatus.NEW, EventStatus.NOT_PRODUCED]),
                    cls.flush_counter < MAX_FLUSH_COUNTER_VALUE,
                ),
            )
            .order_by(cls.status_sorting_case().asc())
            .order_by(cls.id.asc())
            .limit(size)
            .with_for_update()
        )

    @classmethod
    def get_event_query(cls, event_id: uuid.UUID) -> sqlalchemy.Select:
        return (
            sqlalchemy.select(cls)
            .select_from(cls)
            .where(
                sqlalchemy.and_(
                    cls.event_id_bin == func.UUID_TO_BIN(event_id),
                    cls.event_status.in_([EventStatus.NEW, EventStatus.NOT_PRODUCED]),
                    cls.flush_counter < MAX_FLUSH_COUNTER_VALUE,
                ),
            )
            .order_by(cls.status_sorting_case().asc())
            .order_by(cls.id.asc())
            .with_for_update()
        )

    @classmethod
    def update_status_query(
        cls,
        event_id: uuid.UUID,
        status: typing.Literal[
            EventStatus.PRODUCED,
            EventStatus.NOT_PRODUCED,
        ],
    ) -> sqlalchemy.Update:
        values = {"event_status": status}
        if status == EventStatus.NOT_PRODUCED:
            values["flush_counter"] = cls.flush_counter + 1

        return sqlalchemy.update(cls).where(cls.event_id_bin == func.UUID_TO_BIN(event_id)).values(**values)

    @classmethod
    def status_sorting_case(cls) -> sqlalchemy.case:
        return sqlalchemy.case(
            {
                EventStatus.NEW: 1,
                EventStatus.NOT_PRODUCED: 2,
                EventStatus.PRODUCED: 3,
            },
            value=cls.event_status,
            else_=4,
        )


class SqlAlchemyOutbox(protocol.Outbox):
    EVENT_CLASS_MAPPING: typing.ClassVar[typing.Dict[EventType, typing.Type[protocol.Event]]] = {
        EventType.NOTIFICATION_EVENT: ev.NotificationEvent,
        EventType.ECST_EVENT: ev.ECSTEvent,
    }

    def __init__(self, session: sql_session.AsyncSession):
        self.session = session

    def add(self, event: protocol.Event):
        self.session.add(
            OutboxModel(
                event_id=event.event_id,
                event_id_bin=func.UUID_TO_BIN(event.event_id),
                event_type=EventType(event.event_type),
                event_name=event.event_name,
                created_at=event.event_timestamp,
                payload=event.payload,
            ),
        )

    async def save(self):
        await self.session.commit()

    async def get_events(self, batch_size: int = 100) -> typing.List[protocol.Event]:
        events: typing.Sequence[OutboxModel] = (
            (await self.session.execute(OutboxModel.get_batch_query(batch_size))).scalars().all()
        )
        result = []

        for event in events:
            if not self.EVENT_CLASS_MAPPING.get(event.event_type):
                logger.warning(f"Unknown event type for {event}")
                continue
            result.append(self.EVENT_CLASS_MAPPING[event.event_type].model_validate(event))
        return result

    async def get_event(self, event_id: uuid.UUID) -> protocol.Event | None:
        event: OutboxModel | None = (await self.session.execute(OutboxModel.get_event_query(event_id))).scalar()

        if event is None:
            return

        if not self.EVENT_CLASS_MAPPING.get(event.event_type):
            logger.warning(f"Unknown event type for {event}")
            return

        return self.EVENT_CLASS_MAPPING[event.event_type].model_validate(event)

    async def mark_as_produced(self, event_id: uuid.UUID) -> None:
        await self.session.execute(OutboxModel.update_status_query(event_id, EventStatus.PRODUCED))

    async def mark_as_failure(self, event_id: uuid.UUID) -> None:
        await self.session.execute(OutboxModel.update_status_query(event_id, EventStatus.NOT_PRODUCED))

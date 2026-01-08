import datetime
import logging
import typing
import uuid
import orjson
import cqrs
from cqrs import compressors
from cqrs.outbox import map, repository

try:
    import sqlalchemy
    from sqlalchemy import func
    from sqlalchemy.orm import Mapped, mapped_column, declared_attr
    from sqlalchemy.ext.asyncio import session as sql_session
except ImportError:
    raise ImportError(
        "You are trying to use SQLAlchemy outbox implementation, "
        "but 'sqlalchemy' is not installed. "
        "Please install it using: pip install python-cqrs[sqlalchemy]"
    )


logger = logging.getLogger(__name__)
DEFAULT_OUTBOX_TABLE_NAME = "outbox"
MAX_FLUSH_COUNTER_VALUE = 5


class OutboxModelMixin:
    @declared_attr.directive
    def __tablename__(self) -> str:
        return DEFAULT_OUTBOX_TABLE_NAME

    id: Mapped[int] = mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.Identity(),
        primary_key=True,
        nullable=False,
        comment="Identity",
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        sqlalchemy.Uuid,
        nullable=False,
        comment="Event idempotency id",
    )
    event_status: Mapped[repository.EventStatus] = mapped_column(
        sqlalchemy.Enum(repository.EventStatus),
        nullable=False,
        default=repository.EventStatus.NEW,
        comment="Event producing status",
    )
    flush_counter: Mapped[int] = mapped_column(
        sqlalchemy.SmallInteger,
        nullable=False,
        default=0,
        comment="Event producing flush counter",
    )
    event_name: Mapped[typing.Text] = mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Event name",
    )
    topic: Mapped[typing.Text] = mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        default="",
        comment="Event topic",
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        sqlalchemy.DateTime,
        nullable=False,
        server_default=func.now(),
        comment="Event creation timestamp",
    )
    payload: Mapped[bytes] = mapped_column(
        sqlalchemy.LargeBinary,
        nullable=False,
        comment="Event payload",
    )

    @declared_attr
    def __table_args__(self):
        return (
            sqlalchemy.UniqueConstraint(
                "event_id",
                "event_name",
                name="event_id_unique_index",
            ),
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


class SqlAlchemyOutboxedEventRepository(repository.OutboxedEventRepository):
    def __init__(
        self,
        session: sql_session.AsyncSession,
        outbox_model: type[OutboxModelMixin],
        compressor: compressors.Compressor | None = None,
    ):
        self.session = session
        self._compressor = compressor
        self._outbox_model = outbox_model

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
            self._outbox_model(
                event_id=event.event_id,
                event_name=event.event_name,
                created_at=event.event_timestamp,
                payload=bytes_payload,
                topic=event.topic,
            ),
        )

    def _process_events(self, model: OutboxModelMixin) -> repository.OutboxedEvent | None:
        event_dict = model.row_to_dict()

        event_model = map.OutboxedEventMap.get(event_dict["event_name"])
        if event_model is None:
            return None

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
        events: typing.Sequence[OutboxModelMixin] = (
            (await self.session.execute(self._outbox_model.get_batch_query(batch_size, topic)))
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
            statement=self._outbox_model.update_status_query(outboxed_event_id, new_status),
        )

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()

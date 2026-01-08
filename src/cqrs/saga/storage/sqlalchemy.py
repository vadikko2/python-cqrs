import datetime
import logging
import typing
import uuid
from cqrs.dispatcher.exceptions import SagaConcurrencyError
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage

try:
    import sqlalchemy
    from sqlalchemy import func
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from sqlalchemy.orm import registry
except ImportError:
    raise ImportError(
        "You are trying to use SQLAlchemy saga storage implementation, "
        "but 'sqlalchemy' is not installed. "
        "Please install it using: pip install python-cqrs[sqlalchemy]"
    )

Base = registry().generate_base()
logger = logging.getLogger(__name__)

DEFAULT_SAGA_EXECUTION_TABLE_NAME = "saga_executions"
DEFAULT_SAGA_LOG_TABLE_NAME = "saga_logs"


class SagaExecutionModel(Base):
    __tablename__ = DEFAULT_SAGA_EXECUTION_TABLE_NAME

    id = sqlalchemy.Column(
        sqlalchemy.Uuid,
        primary_key=True,
        nullable=False,
        comment="Saga ID",
    )
    name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Saga Name",
    )
    status = sqlalchemy.Column(
        sqlalchemy.Enum(SagaStatus),
        nullable=False,
        default=SagaStatus.PENDING,
        comment="Current status of the saga",
    )
    context = sqlalchemy.Column(
        sqlalchemy.JSON,
        nullable=False,
        comment="Serialized context",
    )
    version = sqlalchemy.Column(
        sqlalchemy.Integer,
        nullable=False,
        default=1,
        comment="Optimistic locking version",
    )
    created_at = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Creation timestamp",
    )
    updated_at = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Last update timestamp",
    )


class SagaLogModel(Base):
    __tablename__ = DEFAULT_SAGA_LOG_TABLE_NAME

    id = sqlalchemy.Column(
        sqlalchemy.BigInteger(),
        sqlalchemy.Identity(),
        primary_key=True,
        nullable=False,
        autoincrement=True,
        comment="Log ID",
    )
    saga_id = sqlalchemy.Column(
        sqlalchemy.Uuid,
        sqlalchemy.ForeignKey(f"{DEFAULT_SAGA_EXECUTION_TABLE_NAME}.id"),
        nullable=False,
        comment="Saga ID",
    )
    step_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        nullable=False,
        comment="Name of the step",
    )
    action = sqlalchemy.Column(
        sqlalchemy.String(50),
        nullable=False,
        comment="Action performed (act/compensate)",
    )
    status = sqlalchemy.Column(
        sqlalchemy.Enum(SagaStepStatus),
        nullable=False,
        comment="Status of the step",
    )
    details = sqlalchemy.Column(
        sqlalchemy.Text,
        nullable=True,
        comment="Additional details or error message",
    )
    created_at = sqlalchemy.Column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Timestamp of the log entry",
    )


class SqlAlchemySagaStorage(ISagaStorage):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        async with self.session_factory() as session:
            try:
                execution = SagaExecutionModel(
                    id=saga_id,
                    name=name,
                    status=SagaStatus.PENDING,
                    context=context,
                    version=1,
                )
                session.add(execution)
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
        current_version: int | None = None,
    ) -> None:
        async with self.session_factory() as session:
            try:
                stmt = sqlalchemy.update(SagaExecutionModel).where(
                    SagaExecutionModel.id == saga_id,
                )

                if current_version is not None:
                    stmt = stmt.where(SagaExecutionModel.version == current_version)
                    stmt = stmt.values(
                        context=context,
                        version=SagaExecutionModel.version + 1,
                    )
                else:
                    # If no version check, just increment version
                    stmt = stmt.values(
                        context=context,
                        version=SagaExecutionModel.version + 1,
                    )

                result = await session.execute(stmt)

                if result.rowcount == 0:
                    # Check if saga exists to distinguish between "not found" and "concurrency error"
                    # But for now, we assume if rowcount is 0 and we checked version, it's concurrency
                    if current_version is not None:
                        # Double check if saga exists
                        check_stmt = sqlalchemy.select(SagaExecutionModel.id).where(
                            SagaExecutionModel.id == saga_id,
                        )
                        check_result = await session.execute(check_stmt)
                        if check_result.scalar_one_or_none():
                            raise SagaConcurrencyError(
                                f"Saga {saga_id} was modified concurrently",
                            )

                    # If we didn't check version or saga doesn't exist, maybe raise not found or ignore?
                    # Original implementation didn't check rowcount.
                    # If version check was requested and failed, we MUST raise error.
                    if current_version is not None:
                        raise SagaConcurrencyError(
                            f"Saga {saga_id} was modified concurrently or does not exist",
                        )

                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        async with self.session_factory() as session:
            try:
                await session.execute(
                    sqlalchemy.update(SagaExecutionModel)
                    .where(SagaExecutionModel.id == saga_id)
                    .values(
                        status=status,
                        version=SagaExecutionModel.version + 1,
                    ),
                )
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        async with self.session_factory() as session:
            try:
                log_entry = SagaLogModel(
                    saga_id=saga_id,
                    step_name=step_name,
                    action=action,
                    status=status,
                    details=details,
                )
                session.add(log_entry)
                await session.commit()
            except SQLAlchemyError:
                await session.rollback()
                raise

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
        *,
        read_for_update: bool = False,
    ) -> tuple[SagaStatus, dict[str, typing.Any], int]:
        async with self.session_factory() as session:
            stmt = sqlalchemy.select(SagaExecutionModel).where(
                SagaExecutionModel.id == saga_id,
            )
            if read_for_update:
                stmt = stmt.with_for_update()

            result = await session.execute(stmt)
            execution = result.scalars().first()
            if not execution:
                raise ValueError(f"Saga {saga_id} not found")

            # Type assertions for SQLAlchemy ORM attributes
            status_value: SagaStatus = typing.cast(SagaStatus, execution.status)
            context_value: dict[str, typing.Any] = typing.cast(
                dict[str, typing.Any],
                execution.context,
            )
            version_value: int = typing.cast(int, execution.version)
            return status_value, context_value, version_value

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        async with self.session_factory() as session:
            result = await session.execute(
                sqlalchemy.select(SagaLogModel)
                .where(SagaLogModel.saga_id == saga_id)
                .order_by(SagaLogModel.created_at),
            )
            rows = result.scalars().all()

            return [
                SagaLogEntry(
                    saga_id=typing.cast(uuid.UUID, row.saga_id),
                    step_name=typing.cast(str, row.step_name),
                    action=typing.cast(typing.Literal["act", "compensate"], row.action),
                    status=typing.cast(SagaStepStatus, row.status),
                    timestamp=typing.cast(
                        datetime.datetime,
                        row.created_at.replace(tzinfo=datetime.timezone.utc)
                        if row.created_at.tzinfo is None
                        else row.created_at,
                    ),
                    details=typing.cast(str | None, row.details),
                )
                for row in rows
            ]

import datetime
import logging
import typing
import uuid

import sqlalchemy
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import registry

from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.models import SagaLogEntry
from cqrs.saga.storage.protocol import ISagaStorage

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
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_saga(
        self,
        saga_id: uuid.UUID,
        name: str,
        context: dict[str, typing.Any],
    ) -> None:
        execution = SagaExecutionModel(
            id=saga_id,
            name=name,
            status=SagaStatus.PENDING,
            context=context,
        )
        self.session.add(execution)
        await self.session.flush()

    async def update_context(
        self,
        saga_id: uuid.UUID,
        context: dict[str, typing.Any],
    ) -> None:
        await self.session.execute(
            sqlalchemy.update(SagaExecutionModel)
            .where(SagaExecutionModel.id == saga_id)
            .values(context=context),
        )

    async def update_status(
        self,
        saga_id: uuid.UUID,
        status: SagaStatus,
    ) -> None:
        await self.session.execute(
            sqlalchemy.update(SagaExecutionModel)
            .where(SagaExecutionModel.id == saga_id)
            .values(status=status),
        )

    async def log_step(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        details: str | None = None,
    ) -> None:
        log_entry = SagaLogModel(
            saga_id=saga_id,
            step_name=step_name,
            action=action,
            status=status,
            details=details,
        )
        self.session.add(log_entry)
        await self.session.flush()

    async def load_saga_state(
        self,
        saga_id: uuid.UUID,
    ) -> tuple[SagaStatus, dict[str, typing.Any]]:
        result = await self.session.execute(
            sqlalchemy.select(SagaExecutionModel).where(
                SagaExecutionModel.id == saga_id,
            ),
        )
        execution = result.scalar_one_or_none()
        if not execution:
            raise ValueError(f"Saga {saga_id} not found")

        # Type assertions for SQLAlchemy ORM attributes
        status_value: SagaStatus = typing.cast(SagaStatus, execution.status)
        context_value: dict[str, typing.Any] = typing.cast(
            dict[str, typing.Any],
            execution.context,
        )
        return status_value, context_value

    async def get_step_history(
        self,
        saga_id: uuid.UUID,
    ) -> list[SagaLogEntry]:
        result = await self.session.execute(
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

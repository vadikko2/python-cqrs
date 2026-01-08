"""Compensation components for Saga transactions."""

import asyncio
import logging
import typing

from cqrs.saga.models import ContextT
from cqrs.saga.step import SagaStepHandler
from cqrs.saga.storage.enums import SagaStepStatus, SagaStatus
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger("cqrs.saga")


class SagaCompensator(typing.Generic[ContextT]):
    """Handles compensation of saga steps with retry mechanism."""

    def __init__(
        self,
        saga_id: typing.Any,
        context: ContextT,
        storage: ISagaStorage,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        retry_backoff: float = 2.0,
    ) -> None:
        """
        Initialize compensator.

        Args:
            saga_id: UUID of the saga
            context: Saga context
            storage: Saga storage implementation
            retry_count: Number of retry attempts for compensation
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Backoff multiplier for exponential delay
        """
        self._saga_id = saga_id
        self._context = context
        self._storage = storage
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._retry_backoff = retry_backoff

    async def compensate_steps(
        self,
        completed_steps: list[SagaStepHandler[ContextT, typing.Any]],
    ) -> None:
        """
        Compensate all completed steps in reverse order with retry mechanism.

        Args:
            completed_steps: List of completed step handlers to compensate
        """
        await self._storage.update_status(self._saga_id, SagaStatus.COMPENSATING)

        # Load history to skip already compensated steps
        history = await self._storage.get_step_history(self._saga_id)
        compensated_steps = {
            e.step_name
            for e in history
            if e.status == SagaStepStatus.COMPLETED and e.action == "compensate"
        }

        compensation_errors: list[
            tuple[SagaStepHandler[ContextT, typing.Any], Exception]
        ] = []

        for step in reversed(completed_steps):
            step_name = step.__class__.__name__

            if step_name in compensated_steps:
                logger.debug(f"Skipping already compensated step: {step_name}")
                continue

            try:
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "compensate",
                    SagaStepStatus.STARTED,
                )

                await self._compensate_step_with_retry(step)

                await self._storage.update_context(
                    self._saga_id,
                    self._context.to_dict(),
                )
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "compensate",
                    SagaStepStatus.COMPLETED,
                )

            except Exception as compensation_error:
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "compensate",
                    SagaStepStatus.FAILED,
                    str(compensation_error),
                )
                # Store both step and error for better error reporting
                compensation_errors.append((step, compensation_error))

        # If compensation failed after all retries
        if compensation_errors:
            for step, comp_error in compensation_errors:
                step_name = step.__class__.__name__
                logger.error(
                    f"Compensation failed for step '{step_name}' after {self._retry_count} attempts. "
                    f"Error: {type(comp_error).__name__}: {comp_error}",
                    exc_info=comp_error,
                )
            # Mark as failed eventually
            await self._storage.update_status(self._saga_id, SagaStatus.FAILED)
        else:
            # If all compensations succeeded (or were skipped), mark as failed
            await self._storage.update_status(self._saga_id, SagaStatus.FAILED)

    async def _compensate_step_with_retry(
        self,
        step: SagaStepHandler[ContextT, typing.Any],
    ) -> None:
        """
        Compensate a single step with retry mechanism and exponential backoff.

        Args:
            step: The step handler to compensate

        Raises:
            Exception: If compensation fails after all retry attempts
        """
        step_name = step.__class__.__name__

        last_exception: Exception | None = None
        for attempt in range(1, self._retry_count + 1):
            try:
                await step.compensate(self._context)
                logger.debug(
                    f"Successfully compensated step '{step_name}' on attempt {attempt}",
                )
                return
            except Exception as e:
                last_exception = e
                if attempt < self._retry_count:
                    # Calculate exponential backoff delay
                    delay = self._retry_delay * (self._retry_backoff ** (attempt - 1))
                    logger.warning(
                        f"Compensation attempt {attempt}/{self._retry_count} failed for step '{step_name}': {e}. "
                        f"Retrying in {delay:.2f}s...",
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Compensation failed for step '{step_name}' after {self._retry_count} attempts: {e}",
                    )

        # If we get here, all retries failed
        if last_exception:
            raise last_exception

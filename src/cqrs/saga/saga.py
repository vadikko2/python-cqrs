import logging
import types
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.saga.compensation import SagaCompensator
from cqrs.saga.execution import (
    FallbackStepExecutor,
    SagaRecoveryManager,
    SagaStateManager,
    SagaStepExecutor,
)
from cqrs.saga.fallback import Fallback
from cqrs.saga.models import ContextT
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.protocol import ISagaStorage
from cqrs.saga.validation import (
    SagaContextTypeExtractor,
    SagaStepValidator,
)

logger = logging.getLogger("cqrs.saga")


class SagaTransaction(typing.Generic[ContextT]):
    """
    Context manager for saga transaction execution.

    Provides an async iterable interface for executing saga steps with automatic
    compensation on failure. Each iteration yields the result of a step's act method.

    Recovery Strategy: Strict Backward Recovery
    --------------------------------------------
    This implementation follows the "Strict Backward Recovery" pattern, which ensures
    data consistency in distributed systems. The key principle is:

    **Point of No Return**: Once a saga enters COMPENSATING or FAILED status,
    forward execution is permanently disabled. Only compensation can proceed.

    Why this matters:
    - Prevents "zombie states" where a saga is partially compensated and partially executed
    - Ensures eventual consistency: if we started rolling back, we must finish rolling back
    - Avoids scenarios where compensation actions (e.g., refund) conflict with new execution
      attempts (e.g., retry of failed step)

    Example scenario:
        Step A (Reserve inventory) - Success
        Step B (Charge payment) - Success
        Step C (Ship order) - Fails
        Saga enters COMPENSATING status
        Compensation C - Success (nothing to undo)
        System crashes during compensation B

        On recovery:
        - Status is COMPENSATING
        - We MUST complete compensation (refund payment, release inventory)
        - We MUST NOT retry Step C, even if network is now available
        - This prevents: client gets free product (refunded + shipped)

    Usage::

        async with saga.transaction(context=OrderContext(order_id="123")) as transaction:
            async for step_result in transaction:
                # Process step_result
                pass
    """

    def __init__(
        self,
        saga: "Saga[ContextT]",
        context: ContextT,
        container: Container,
        storage: ISagaStorage,
        saga_id: uuid.UUID | None = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
    ):
        self._saga = saga
        self._context = context
        self._container = container
        self._storage = storage
        self._completed_steps: list[SagaStepHandler[ContextT, typing.Any]] = []
        self._error: BaseException | None = None
        self._compensated: bool = False

        self._saga_id = saga_id or uuid.uuid4()
        self._is_new_saga = saga_id is None

        # Initialize components
        self._state_manager = SagaStateManager(self._saga_id, storage)
        self._recovery_manager = SagaRecoveryManager(
            self._saga_id,
            storage,
            container,
            saga.steps,
        )
        self._step_executor: SagaStepExecutor[ContextT] = SagaStepExecutor[ContextT](
            context,
            container,
            self._state_manager,
        )
        self._fallback_executor: FallbackStepExecutor[ContextT] = FallbackStepExecutor[
            ContextT
        ](
            context,
            container,
            self._state_manager,
        )
        self._compensator: SagaCompensator[ContextT] = SagaCompensator[ContextT](
            self._saga_id,
            context,
            storage,
            compensation_retry_count,
            compensation_retry_delay,
            compensation_retry_backoff,
        )

    @property
    def saga_id(self) -> uuid.UUID:
        return self._saga_id

    @property
    def completed_steps(self) -> list[SagaStepHandler[ContextT, typing.Any]]:
        """
        Get list of completed step handlers.

        Returns:
            List of step handlers that have been executed successfully.
            Can be used to collect events from steps.
        """
        return self._completed_steps

    async def __aenter__(self) -> "SagaTransaction[ContextT]":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> bool:
        # If an exception occurred, compensate all completed steps
        # Only compensate if not already compensated in __aiter__
        if exc_val is not None and not self._compensated:
            self._error = exc_val
            await self._compensate()
        return False  # Don't suppress the exception

    async def __aiter__(
        self,
    ) -> typing.AsyncIterator[SagaStepResult[ContextT, typing.Any]]:
        """
        Execute saga steps sequentially and yield each step result.

        This method implements the "Strict Backward Recovery" strategy for saga execution.
        Once a saga enters COMPENSATING or FAILED status, it can never proceed forward.
        This ensures data consistency and prevents "zombie states" where a saga is
        partially compensated and partially executed.

        Strategy Overview:
        - Forward Execution (RUNNING/PENDING): Execute steps sequentially, skipping
          already completed steps for idempotency.
        - Point of No Return: If saga status is COMPENSATING or FAILED, immediately
          resume compensation without attempting forward execution. This prevents
          inconsistent states where partial compensation conflicts with new execution.
        - Local Retries: Retry logic is handled at the step level (within step.act()).
          While retrying, saga status remains RUNNING, allowing recovery to continue
          forward execution if the retry succeeds.
        - Global Failure: Once all local retries are exhausted and saga transitions
          to COMPENSATING, the path forward is permanently closed. Only compensation
          can proceed.

        Yields:
            SagaStepResult for each successfully executed step.

        Raises:
            Exception: If any step fails, compensation is triggered and
                      the exception is re-raised. Also raised when recovering
                      a saga in COMPENSATING/FAILED status.
        """
        # 1. Initialization / Recovery
        completed_step_names: set[str] = set()

        if self._is_new_saga:
            await self._state_manager.create_saga(
                self._saga.__class__.__name__,
                self._context,
            )
            await self._state_manager.update_status(SagaStatus.RUNNING)
        else:
            # Try to recover state
            try:
                status, _, _ = await self._storage.load_saga_state(
                    self._saga_id,
                    read_for_update=True,
                )

                # Check for terminal states first
                if status == SagaStatus.COMPLETED:
                    logger.info(
                        f"Saga {self._saga_id} is already {status}. Skipping execution.",
                    )
                    return

                # POINT OF NO RETURN: Strict Backward Recovery Strategy
                if status in (SagaStatus.COMPENSATING, SagaStatus.FAILED):
                    logger.warning(
                        f"Saga {self._saga_id} is in {status} state. "
                        "Resuming compensation immediately.",
                    )

                    # Restore completed steps from history for compensation
                    completed_act_steps = (
                        await self._recovery_manager.load_completed_step_names()
                    )
                    reconstructed_steps = (
                        await self._recovery_manager.reconstruct_completed_steps(
                            completed_act_steps,
                        )
                    )
                    # Type cast is safe here because steps are reconstructed from the same saga
                    # that uses ContextT, so they have the correct context type
                    # We need to rebuild the list to satisfy type checker's invariance requirements
                    self._completed_steps = [
                        typing.cast(SagaStepHandler[ContextT, typing.Any], step)
                        for step in reconstructed_steps
                    ]

                    # Immediately proceed to compensation - no forward execution
                    await self._compensate()

                    # Raise exception to signal that saga was recovered in failed state
                    raise RuntimeError(
                        f"Saga {self._saga_id} was recovered in {status} state "
                        "and compensation was completed. Forward execution is not allowed.",
                    )

                # For RUNNING/PENDING status, load history to skip completed steps
                completed_step_names = (
                    await self._recovery_manager.load_completed_step_names()
                )
            except ValueError:
                # If loading fails but ID was provided, create it
                await self._state_manager.create_saga(
                    self._saga.__class__.__name__,
                    self._context,
                )
                await self._state_manager.update_status(SagaStatus.RUNNING)

        step_name = "unknown_step"
        try:
            for step_item in self._saga.steps:
                # Check if this is a Fallback wrapper
                if isinstance(step_item, Fallback):
                    (
                        step_result,
                        executed_step,
                    ) = await self._fallback_executor.execute_fallback_step(
                        step_item,
                        completed_step_names,
                    )
                    if step_result is not None and executed_step is not None:
                        # Track completed step for compensation
                        self._completed_steps.append(executed_step)
                        yield step_result
                    elif executed_step is None:
                        # Step was skipped (already completed), restore it for compensation
                        primary_name = step_item.step.__name__
                        fallback_name = step_item.fallback.__name__
                        if primary_name in completed_step_names:
                            step = await self._container.resolve(step_item.step)
                            self._completed_steps.append(step)
                        elif fallback_name in completed_step_names:
                            step = await self._container.resolve(step_item.fallback)
                            self._completed_steps.append(step)
                    continue

                # Regular step handling
                step_type = step_item
                step_name = step_type.__name__

                # 2. Skip logic (Idempotency)
                if step_name in completed_step_names:
                    # Restore step instance to completed_steps for potential compensation
                    step = await self._container.resolve(step_type)
                    self._completed_steps.append(step)
                    logger.debug(f"Skipping already completed step: {step_name}")
                    continue

                # 3. Execution
                step_result = await self._step_executor.execute_step(
                    step_type,
                    step_name,
                )

                # Track completed step for compensation
                step = await self._container.resolve(step_type)
                self._completed_steps.append(step)

                yield step_result

            # Update context one final time before marking as completed
            await self._state_manager.update_context(self._context)
            await self._state_manager.update_status(SagaStatus.COMPLETED)

        except Exception as e:
            # Log failure for the specific step
            await self._state_manager.log_step(
                step_name,
                "act",
                SagaStepStatus.FAILED,
                str(e),
            )
            self._error = e
            await self._compensate()
            raise

    async def _compensate(self) -> None:
        """Compensate all completed steps in reverse order with retry mechanism."""
        # Prevent double compensation
        if self._compensated:
            return

        self._compensated = True
        await self._compensator.compensate_steps(self._completed_steps)


class Saga(typing.Generic[ContextT]):
    """
    Saga class that defines steps and context type.

    Saga is a simple class that serves as a container for:
    - List of step handler types to execute in order
    - Context type (via Generic parameter)

    Saga does not depend on DI container or storage. All work with dependencies
    and storage is handled by SagaTransaction class.

    Steps must be defined as a class attribute. All steps must handle the same
    context type as specified in the Generic parameter.

    Usage::

        class OrderSaga(Saga[OrderContext]):
            steps = [
                ReserveInventoryStep,
                ProcessPaymentStep,
                ShipOrderStep,
            ]

        saga = OrderSaga()

    Note:
        Steps are validated at class creation time via __init_subclass__ to ensure
        they handle the correct context type.
    """

    steps: typing.ClassVar[list[type[SagaStepHandler] | Fallback]] = []

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        """Validate steps when subclass is created."""
        super().__init_subclass__(**kwargs)
        cls._validate_steps()

    @classmethod
    def _validate_steps(cls) -> None:
        """
        Validate saga steps.

        Ensures that:
        1. Steps is a list
        2. All steps are valid step handler types or Fallback instances
        3. All steps handle the correct context type
        """
        # Extract context type from Generic parameter
        context_type = SagaContextTypeExtractor.extract_from_class(cls, Saga)

        # Create validator and validate steps
        validator = SagaStepValidator(
            saga_name=cls.__name__,
            context_type=context_type,
        )
        validator.validate_steps(cls.steps)

    @property
    def steps_count(self) -> int:
        """
        Return the number of steps in the saga.

        Returns:
            The total number of steps configured for this saga.
        """
        return len(self.steps)

    def transaction(
        self,
        context: ContextT,
        container: Container,
        storage: ISagaStorage,
        saga_id: uuid.UUID | None = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
    ) -> SagaTransaction[ContextT]:
        """
        Create a transaction context manager for saga execution.

        The transaction provides an async iterable interface where each iteration
        yields the result of a step's act method. If any step fails or an exception
        occurs, all completed steps are automatically compensated.

        Args:
            context: The saga context object that contains shared state
                    across all steps in the saga.
            container: DI container for resolving step handlers
            storage: Saga storage implementation
            saga_id: Optional UUID for the saga. If provided, it can be used
                     for recovery or ensuring idempotency.
            compensation_retry_count: Number of retry attempts for compensation (default: 3)
            compensation_retry_delay: Initial delay between retries in seconds (default: 1.0)
            compensation_retry_backoff: Backoff multiplier for exponential delay (default: 2.0)

        Returns:
            A SagaTransaction context manager that can be used in an async with statement.

        Usage::

            async with saga.transaction(
                context=OrderContext(order_id="123"),
                container=container,
                storage=storage,
            ) as transaction:
                async for step_result in transaction:
                    # Process step_result
                    pass
        """
        return SagaTransaction(
            saga=self,
            context=context,
            container=container,
            storage=storage,
            saga_id=saga_id,
            compensation_retry_count=compensation_retry_count,
            compensation_retry_delay=compensation_retry_delay,
            compensation_retry_backoff=compensation_retry_backoff,
        )

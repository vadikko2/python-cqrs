import asyncio
import logging
import types
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.middlewares.base import MiddlewareChain
from cqrs.saga.models import ContextT, SagaContext
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger("cqrs.saga")


def _serialize_context(context: SagaContext) -> dict[str, typing.Any]:
    """
    Serialize context to dictionary.

    Supports both SagaContext (with to_dict/model_dump) and Pydantic models.
    """
    if isinstance(context, SagaContext):
        return context.to_dict()
    # Check for model_dump (Pydantic models)
    if hasattr(context, "model_dump"):
        return typing.cast(
            typing.Callable[[], dict[str, typing.Any]],
            context.model_dump,
        )()
    # Fallback to to_dict if available
    if hasattr(context, "to_dict"):
        return typing.cast(
            typing.Callable[[], dict[str, typing.Any]],
            context.to_dict,
        )()
    # Last resort: use __dict__
    return {
        key: value for key, value in context.__dict__.items() if not key.startswith("_")
    }


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
        saga_id: uuid.UUID | None = None,
    ):
        self._saga = saga
        self._context = context
        self._completed_steps: list[SagaStepHandler[ContextT, typing.Any]] = []
        self._error: BaseException | None = None
        self._compensated: bool = False

        self._storage = saga.storage
        self._saga_id = saga_id or uuid.uuid4()
        # If saga_id was passed, we assume it's an existing saga if it exists in storage,
        # but here we treat it as new if not checking storage explicitly.
        # Ideally, we should check storage in __aiter__.
        self._is_new_saga = saga_id is None

    @property
    def saga_id(self) -> uuid.UUID:
        return self._saga_id

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

        # Determine if we need to create or load
        # If ID was provided, we check if it exists (basic check logic)
        # For simplicity, if _is_new_saga is True, we create.
        # If it was False, we try to load.

        if self._is_new_saga:
            await self._storage.create_saga(
                self._saga_id,
                self._saga.__class__.__name__,
                _serialize_context(self._context),
            )
            await self._storage.update_status(self._saga_id, SagaStatus.RUNNING)
        else:
            # Try to recover state
            try:
                status, _ = await self._storage.load_saga_state(self._saga_id)

                # Check for terminal states first
                if status == SagaStatus.COMPLETED:
                    logger.info(
                        f"Saga {self._saga_id} is already {status}. Skipping execution.",
                    )
                    return

                # POINT OF NO RETURN: Strict Backward Recovery Strategy
                # If saga is in COMPENSATING or FAILED status, we must complete compensation
                # and never attempt forward execution. This prevents inconsistent states
                # where partial compensation conflicts with new execution attempts.
                if status in (SagaStatus.COMPENSATING, SagaStatus.FAILED):
                    logger.warning(
                        f"Saga {self._saga_id} is in {status} state. "
                        "Resuming compensation immediately.",
                    )

                    # Restore completed steps from history for compensation
                    history = await self._storage.get_step_history(self._saga_id)
                    completed_act_steps = {
                        e.step_name
                        for e in history
                        if e.status == SagaStepStatus.COMPLETED and e.action == "act"
                    }

                    # Reconstruct completed_steps list in order for proper compensation
                    for step_type in self._saga.steps:
                        step_name = step_type.__name__
                        if step_name in completed_act_steps:
                            step = await self._saga.container.resolve(step_type)
                            self._completed_steps.append(step)

                    # Immediately proceed to compensation - no forward execution
                    await self._compensate()

                    # Raise exception to signal that saga was recovered in failed state
                    raise RuntimeError(
                        f"Saga {self._saga_id} was recovered in {status} state "
                        "and compensation was completed. Forward execution is not allowed.",
                    )

                # For RUNNING/PENDING status, load history to skip completed steps
                history = await self._storage.get_step_history(self._saga_id)
                completed_step_names = {
                    e.step_name
                    for e in history
                    if e.status == SagaStepStatus.COMPLETED and e.action == "act"
                }
            except ValueError:
                # If loading fails but ID was provided, maybe treat as new?
                # Or raise error. Assuming strict consistency for now.
                # If the user provided an ID that doesn't exist, create it.
                await self._storage.create_saga(
                    self._saga_id,
                    self._saga.__class__.__name__,
                    _serialize_context(self._context),
                )
                await self._storage.update_status(self._saga_id, SagaStatus.RUNNING)

        step_name = "unknown_step"
        try:
            for step_type in self._saga.steps:
                step_name = step_type.__name__

                # 2. Skip logic (Idempotency)
                if step_name in completed_step_names:
                    # Restore step instance to completed_steps for potential compensation
                    step = await self._saga.container.resolve(step_type)
                    self._completed_steps.append(step)
                    logger.debug(f"Skipping already completed step: {step_name}")
                    continue

                # Resolve step handler from DI container
                step = await self._saga.container.resolve(step_type)

                # 3. Execution
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "act",
                    SagaStepStatus.STARTED,
                )

                step_result = await step.act(self._context)

                self._completed_steps.append(step)

                # 4. Commit State
                await self._storage.update_context(
                    self._saga_id,
                    _serialize_context(self._context),
                )
                await self._storage.log_step(
                    self._saga_id,
                    step_name,
                    "act",
                    SagaStepStatus.COMPLETED,
                )

                yield step_result

            await self._storage.update_status(self._saga_id, SagaStatus.COMPLETED)

        except Exception as e:
            # Log failure for the specific step
            await self._storage.log_step(
                self._saga_id,
                step_name,
                "act",
                SagaStepStatus.FAILED,
                str(e),
            )
            self._error = e
            await self._compensate()
            raise

    async def _compensate(self) -> None:
        """
        Compensate all completed steps in reverse order with retry mechanism.
        """
        # Prevent double compensation
        if self._compensated:
            return

        self._compensated = True

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

        for step in reversed(self._completed_steps):
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
                    _serialize_context(self._context),
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
                    f"Compensation failed for step '{step_name}' after {self._saga.compensation_retry_count} attempts. "
                    f"Error: {type(comp_error).__name__}: {comp_error}",
                    exc_info=comp_error,
                )
            # Mark as failed eventually
            await self._storage.update_status(self._saga_id, SagaStatus.FAILED)
        else:
            # If all compensations succeeded (or were skipped), we effectively failed the saga transaction
            # but successfully compensated. The saga status is FAILED.
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
        for attempt in range(1, self._saga.compensation_retry_count + 1):
            try:
                await step.compensate(self._context)
                logger.debug(
                    f"Successfully compensated step '{step_name}' on attempt {attempt}",
                )
                return
            except Exception as e:
                last_exception = e
                if attempt < self._saga.compensation_retry_count:
                    # Calculate exponential backoff delay
                    delay = self._saga.compensation_retry_delay * (
                        self._saga.compensation_retry_backoff ** (attempt - 1)
                    )
                    logger.warning(
                        f"Compensation attempt {attempt}/{self._saga.compensation_retry_count} failed for step '{step_name}': {e}. "
                        f"Retrying in {delay:.2f}s...",
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Compensation failed for step '{step_name}' after {self._saga.compensation_retry_count} attempts: {e}",
                    )

        # If we get here, all retries failed
        if last_exception:
            raise last_exception


class Saga(typing.Generic[ContextT]):
    """
    A saga orchestrator that executes steps sequentially and handles compensation.

    The saga pattern enables distributed transactions by executing a series of steps
    where each step can be compensated if a subsequent step fails. This allows
    for eventual consistency across distributed systems without requiring two-phase commit.

    Recovery Strategy: Strict Backward Recovery
    --------------------------------------------
    This saga implementation uses "Strict Backward Recovery" to ensure data consistency:

    1. **Forward Execution**: Steps execute sequentially. If a step fails, compensation
       begins immediately for all completed steps.

    2. **Point of No Return**: Once saga status becomes COMPENSATING or FAILED,
       forward execution is permanently disabled. Recovery will only complete compensation,
       never retry failed steps.

    3. **Local Retries**: Retry logic should be implemented within step.act() methods.
       While retrying (even with failures), saga status remains RUNNING, allowing
       recovery to continue forward execution if retry eventually succeeds.

    4. **Global Failure**: When all local retries are exhausted and step.act() raises
       a fatal exception, saga transitions to COMPENSATING. From this point, only
       compensation can proceed.

    This strategy prevents inconsistent states where partial compensation conflicts
    with new execution attempts, which could lead to data loss or financial discrepancies.

    Step handlers are resolved from the DI container, allowing for dependency injection
    of services and repositories into each step handler.

    Usage::

        steps = [
            ReserveInventoryStep,
            ProcessPaymentStep,
            ShipOrderStep,
        ]
        saga = Saga(steps=steps, container=container)

        # Execute saga using transaction context manager
        async with saga.transaction(context=OrderContext(order_id="123")) as transaction:
            async for step_result in transaction:
                # Process each step result
                print(f"Step {step_result.step_type.__name__} completed")
    """

    def __init__(
        self,
        steps: list[typing.Type[SagaStepHandler[ContextT, typing.Any]]],
        container: Container,
        storage: ISagaStorage | None = None,
        middleware_chain: MiddlewareChain | None = None,
        compensation_retry_count: int = 3,
        compensation_retry_delay: float = 1.0,
        compensation_retry_backoff: float = 2.0,
    ):
        """
        Initialize a saga orchestrator.

        Args:
            steps: List of step handler types to execute in order
            container: DI container for resolving step handlers
            storage: Saga persistence storage. Defaults to MemorySagaStorage.
            middleware_chain: Optional middleware chain for request processing
            compensation_retry_count: Number of retry attempts for compensation (default: 3)
            compensation_retry_delay: Initial delay between retries in seconds (default: 1.0)
            compensation_retry_backoff: Backoff multiplier for exponential delay (default: 2.0)
        """
        self._steps = steps
        self._container = container
        self._storage = storage or MemorySagaStorage()
        self._middleware_chain = middleware_chain
        self._compensation_retry_count = compensation_retry_count
        self._compensation_retry_delay = compensation_retry_delay
        self._compensation_retry_backoff = compensation_retry_backoff

    @property
    def compensation_retry_count(self) -> int:
        """Return the number of retry attempts for compensation."""
        return self._compensation_retry_count

    @property
    def compensation_retry_delay(self) -> float:
        """Return the initial delay between compensation retries in seconds."""
        return self._compensation_retry_delay

    @property
    def compensation_retry_backoff(self) -> float:
        """Return the backoff multiplier for exponential delay between retries."""
        return self._compensation_retry_backoff

    @property
    def steps_count(self) -> int:
        """
        Return the number of steps in the saga.

        Returns:
            The total number of steps configured for this saga.
        """
        return len(self._steps)

    @property
    def steps(self):
        """
        Return the saga steps

        Returns:
            The steps configured for this saga
        """
        return self._steps

    @property
    def container(self) -> Container:
        """
        Return configured DI container

        Returns:
            The configured DI container
        """
        return self._container

    @property
    def storage(self) -> ISagaStorage:
        """
        Return configured storage
        """
        return self._storage

    def transaction(
        self,
        context: ContextT,
        saga_id: uuid.UUID | None = None,
    ) -> SagaTransaction[ContextT]:
        """
        Create a transaction context manager for saga execution.

        The transaction provides an async iterable interface where each iteration
        yields the result of a step's act method. If any step fails or an exception
        occurs, all completed steps are automatically compensated.

        Args:
            context: The saga context object that contains shared state
                    across all steps in the saga.
            saga_id: Optional UUID for the saga. If provided, it can be used
                     for recovery or ensuring idempotency.

        Returns:
            A SagaTransaction context manager that can be used in an async with statement.

        Usage::

            async with saga.transaction(context=OrderContext(order_id="123")) as transaction:
                async for step_result in transaction:
                    # Process step_result
                    pass
        """
        return SagaTransaction(saga=self, context=context, saga_id=saga_id)

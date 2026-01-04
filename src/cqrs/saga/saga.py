import asyncio
import logging
import types
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.saga.models import ContextT
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.enums import SagaStatus, SagaStepStatus
from cqrs.saga.storage.protocol import ISagaStorage

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
        self._compensation_retry_count = compensation_retry_count
        self._compensation_retry_delay = compensation_retry_delay
        self._compensation_retry_backoff = compensation_retry_backoff

        self._saga_id = saga_id or uuid.uuid4()
        # If saga_id was passed, we assume it's an existing saga if it exists in storage,
        # but here we treat it as new if not checking storage explicitly.
        # Ideally, we should check storage in __aiter__.
        self._is_new_saga = saga_id is None

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

        # Determine if we need to create or load
        # If ID was provided, we check if it exists (basic check logic)
        # For simplicity, if _is_new_saga is True, we create.
        # If it was False, we try to load.

        if self._is_new_saga:
            await self._storage.create_saga(
                self._saga_id,
                self._saga.__class__.__name__,
                self._context.to_dict(),
            )
            await self._storage.update_status(self._saga_id, SagaStatus.RUNNING)
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
                            step = await self._container.resolve(step_type)
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
                    self._context.to_dict(),
                )
                await self._storage.update_status(self._saga_id, SagaStatus.RUNNING)

        step_name = "unknown_step"
        try:
            for step_type in self._saga.steps:
                step_name = step_type.__name__

                # 2. Skip logic (Idempotency)
                if step_name in completed_step_names:
                    # Restore step instance to completed_steps for potential compensation
                    step = await self._container.resolve(step_type)
                    self._completed_steps.append(step)
                    logger.debug(f"Skipping already completed step: {step_name}")
                    continue

                # Resolve step handler from DI container
                step = await self._container.resolve(step_type)

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
                    self._context.to_dict(),
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
                    f"Compensation failed for step '{step_name}' after {self._compensation_retry_count} attempts. "
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
        for attempt in range(1, self._compensation_retry_count + 1):
            try:
                await step.compensate(self._context)
                logger.debug(
                    f"Successfully compensated step '{step_name}' on attempt {attempt}",
                )
                return
            except Exception as e:
                last_exception = e
                if attempt < self._compensation_retry_count:
                    # Calculate exponential backoff delay
                    delay = self._compensation_retry_delay * (
                        self._compensation_retry_backoff ** (attempt - 1)
                    )
                    logger.warning(
                        f"Compensation attempt {attempt}/{self._compensation_retry_count} failed for step '{step_name}': {e}. "
                        f"Retrying in {delay:.2f}s...",
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Compensation failed for step '{step_name}' after {self._compensation_retry_count} attempts: {e}",
                    )

        # If we get here, all retries failed
        if last_exception:
            raise last_exception


class Saga(typing.Generic[ContextT]):
    """
    Declarative saga class that defines steps and context type.

    Saga is a simple declarative class that serves as a container for:
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

    Note:
        Steps are validated at class creation time to ensure they handle
        the correct context type.
    """

    steps: typing.ClassVar[list[typing.Type[SagaStepHandler]]]

    # Steps should be defined as a class attribute in subclasses
    # Example: steps = [Step1, Step2, ...]
    # This is validated in __init_subclass__

    def __init_subclass__(cls, **kwargs: typing.Any) -> None:
        """
        Validate saga steps when subclass is created.

        Ensures that:
        1. Steps attribute is defined
        2. All steps handle the correct context type
        """
        super().__init_subclass__(**kwargs)

        # Get the context type from Generic parameter
        # This works by checking the __orig_bases__ for the Generic type
        context_type: type[ContextT] | None = None

        # Try to get context type from __orig_bases__
        if hasattr(cls, "__orig_bases__"):
            for base in cls.__orig_bases__:  # type: ignore[attr-defined]
                # Check if this is a GenericAlias for Saga
                if isinstance(base, types.GenericAlias) and base.__origin__ is Saga:  # type: ignore[attr-defined]
                    args = typing.get_args(base)
                    if args:
                        context_type = args[0]  # type: ignore[assignment]
                        break
                # Fallback for older Python versions or different typing implementations
                elif hasattr(base, "__origin__") and base.__origin__ is Saga:  # type: ignore[attr-defined]
                    args = typing.get_args(base)
                    if args:
                        context_type = args[0]  # type: ignore[assignment]
                        break

        # If we couldn't determine context type from Generic, try alternative methods
        if context_type is None:
            # Try to get it from __class_getitem__ result
            if hasattr(cls, "__args__") and cls.__args__:  # type: ignore[attr-defined]
                context_type = cls.__args__[0]  # type: ignore[assignment,index]

        # Check if steps attribute exists
        if not hasattr(cls, "steps"):
            raise TypeError(
                f"{cls.__name__} must define 'steps' as a class attribute. "
                "Example: steps = [Step1, Step2, ...]",
            )

        steps = getattr(cls, "steps", [])

        if not isinstance(steps, list):
            raise TypeError(
                f"{cls.__name__}.steps must be a list of step handler types, "
                f"got {type(steps).__name__}",
            )

        if not steps:
            # Empty steps list is allowed (though unusual)
            return

        # Validate each step
        for i, step_type in enumerate(steps):
            if not isinstance(step_type, type):
                raise TypeError(
                    f"{cls.__name__}.steps[{i}] must be a class type, "
                    f"got {type(step_type).__name__}",
                )

            # Check if step is a subclass of SagaStepHandler
            if not issubclass(step_type, SagaStepHandler):
                raise TypeError(
                    f"{cls.__name__}.steps[{i}] ({step_type.__name__}) "
                    "must be a subclass of SagaStepHandler",
                )

            # Try to validate context type compatibility
            # We check the __orig_bases__ of the step handler to see what context it expects
            if context_type is not None:
                step_context_type: type | None = None

                # Try to get step's context type from its generic bases
                if hasattr(step_type, "__orig_bases__"):
                    for base in step_type.__orig_bases__:  # type: ignore[attr-defined]
                        # Check if this is a GenericAlias for SagaStepHandler
                        if (
                            isinstance(base, types.GenericAlias)
                            and base.__origin__ is SagaStepHandler
                        ):  # type: ignore[attr-defined]
                            args = typing.get_args(base)
                            if args:
                                step_context_type = args[0]
                                break
                        # Fallback
                        elif (
                            hasattr(base, "__origin__")
                            and base.__origin__ is SagaStepHandler
                        ):  # type: ignore[attr-defined]
                            args = typing.get_args(base)
                            if args:
                                step_context_type = args[0]
                                break

                # If we found the step's context type, validate it matches
                if step_context_type is not None:
                    # Get origin types to handle type variables and unions
                    origin_context = getattr(context_type, "__origin__", context_type)
                    origin_step_context = getattr(
                        step_context_type,
                        "__origin__",
                        step_context_type,
                    )

                    # Check if types match exactly
                    if origin_context != origin_step_context:
                        # Check if they're compatible types (subclass relationship)
                        # This allows subclasses of the expected context type
                        if isinstance(origin_context, type) and isinstance(
                            origin_step_context,
                            type,
                        ):
                            if not issubclass(origin_context, origin_step_context):
                                raise TypeError(
                                    f"{cls.__name__}.steps[{i}] ({step_type.__name__}) "
                                    f"expects context type {step_context_type.__name__}, "
                                    f"but saga expects {context_type.__name__}. "
                                    "Steps must handle the same context type as the saga.",
                                )
                        else:
                            # For non-type origins (like TypeVar), we can't validate at class creation time
                            # but we log a warning
                            logger.warning(
                                f"{cls.__name__}.steps[{i}] ({step_type.__name__}) "
                                f"may have incompatible context type. "
                                f"Saga expects {context_type.__name__}, "
                                f"step expects {step_context_type.__name__}.",
                            )

    def __init__(self) -> None:
        """
        Initialize a declarative saga instance.

        Steps must be defined as a class attribute, not passed to __init__.
        """
        # Ensure steps attribute exists (should be set as class attribute)
        if not hasattr(self, "steps"):
            raise TypeError(
                f"{self.__class__.__name__} must define 'steps' as a class attribute. "
                "Example: steps = [Step1, Step2, ...]",
            )

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

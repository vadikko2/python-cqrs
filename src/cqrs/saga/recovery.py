import logging
import typing
import uuid

from cqrs.container.protocol import Container
from cqrs.saga.models import ContextT
from cqrs.saga.saga import Saga
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.protocol import ISagaStorage

logger = logging.getLogger(__name__)


async def recover_saga(
    saga: Saga[ContextT],
    saga_id: uuid.UUID,
    context_builder: typing.Union[
        typing.Type[ContextT],
        typing.Callable[[dict[str, typing.Any]], ContextT],
    ],
    container: Container,
    storage: ISagaStorage,
) -> None:
    """
    Recover and complete a potentially interrupted saga.

    This function attempts to resume a saga from where it left off.
    It relies on the storage to load the last known state and execution history.
    Already completed steps will be skipped.
    If the saga was in a compensating state, compensation will resume.

    Args:
        saga: The saga orchestrator instance.
        saga_id: The ID of the saga to recover.
        context_builder: A class type or factory function to reconstruct the context.
                        If a class is provided, it will be instantiated with **data
                        (assuming the constructor accepts kwargs).
                        If a function is provided, it will be called with the data dict.
                        Examples:
                            - MyPydanticModel.model_validate
                            - lambda d: MyDataClass(**d)
                            - MyClass (if __init__ accepts **kwargs)
        container: DI container for resolving step handlers.
        storage: Saga storage implementation.
    """
    # 1. Load state
    try:
        status, context_data, _ = await storage.load_saga_state(saga_id)
    except Exception as e:
        logger.error(f"Failed to load saga {saga_id}: {e}")
        raise

    # Check for terminal states
    # COMPLETED: saga finished successfully, nothing to do
    # FAILED: saga failed and compensation was completed, but we still need to
    #         check if there are any remaining compensation steps to complete
    #         (in case of crash during compensation)
    if status == SagaStatus.COMPLETED:
        logger.info(f"Saga {saga_id} is already completed. Nothing to recover.")
        return

    # 2. Reconstruct context
    try:
        if isinstance(context_builder, type):
            # It's a class, check if it has from_dict method (SagaContext)
            if hasattr(context_builder, "from_dict"):
                context = context_builder.from_dict(context_data)
            else:
                # Fallback: assume constructor accepts kwargs
                context = context_builder(**context_data)
        else:
            # It's a factory function/method
            callable_builder = typing.cast(
                typing.Callable[[dict[str, typing.Any]], ContextT],
                context_builder,
            )
            context = callable_builder(context_data)
    except Exception as e:
        logger.error(f"Failed to reconstruct context for saga {saga_id}: {e}")
        raise

    logger.info(f"Recovering saga {saga_id} (status: {status})...")

    # 3. Resume execution
    # The transaction logic handles skipping completed steps and resuming compensation.
    try:
        async with saga.transaction(
            context=typing.cast(ContextT, context),
            container=container,
            storage=storage,
            saga_id=saga_id,
        ) as transaction:
            async for step_result in transaction:
                logger.info(
                    f"Recovered/Executed step: {step_result.step_type.__name__}",
                )

        logger.info(f"Saga {saga_id} recovery completed successfully.")

    except RuntimeError as e:
        # RuntimeError is raised when saga is recovered in COMPENSATING/FAILED state
        # This is expected behavior - compensation was completed, but we signal
        # that forward execution was not allowed
        error_msg = str(e)
        if "recovered in" in error_msg and "state" in error_msg:
            logger.warning(
                f"Saga {saga_id} recovery completed compensation. "
                "Forward execution was not allowed.",
            )
            # Re-raise to allow callers to handle this case
            raise
        # For other RuntimeErrors, log and re-raise
        logger.error(f"Saga {saga_id} recovery ended with error: {e}")
        raise
    except Exception as e:
        logger.error(f"Saga {saga_id} recovery ended with error: {e}")
        # The transaction handles exception and runs compensation, so the saga state
        # should be updated to FAILED (or COMPENSATED) in storage.
        raise

"""Benchmarks for Saga with Fallback (Pydantic Response)."""

import pytest
from cqrs.adapters.circuit_breaker import AioBreakerAdapter
from cqrs.events.event import Event
from cqrs.saga.fallback import Fallback
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage

from .test_benchmark_saga_memory import (
    OrderContext,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    ReserveInventoryStep,
    SagaContainer,
    ShipOrderStep,
)


class FallbackReserveStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    """Fallback step used when primary fails (not used in happy-path benchmark)."""

    def __init__(self) -> None:
        """
        Initialize the fallback step and prepare storage for collected Event instances.
        
        Creates a private `_events` list used to accumulate Event objects; access via the `events` property.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Get a copy of the recorded Event objects.
        
        Returns:
            list[Event]: A shallow copy of the internal events list containing the recorded Event instances in insertion order.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """
        Create a ReserveInventoryResponse for the given order and return it wrapped in a SagaStepResult.
        
        Parameters:
            context (OrderContext): Saga context containing the order_id used to construct the fallback inventory_id.
        
        Returns:
            SagaStepResult[OrderContext, ReserveInventoryResponse]: Step result containing a `ReserveInventoryResponse` with `reserved=True` and an `inventory_id` derived from the `order_id`.
        """
        response = ReserveInventoryResponse(
            inventory_id=f"fallback_inv_{context.order_id}",
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        No-op compensation for the fallback reserve step.
        
        Parameters:
            context (OrderContext): Saga execution context (not used).
        """
        pass


@pytest.fixture
def saga_container_fallback() -> SagaContainer:
    """
    Create and return a SagaContainer pre-registered with steps used by the fallback benchmarks.
    
    The container registers: ReserveInventoryStep, FallbackReserveStep, ProcessPaymentStep, and ShipOrderStep.
    
    Returns:
        SagaContainer: A container configured with the above saga step instances.
    """
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(FallbackReserveStep, FallbackReserveStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def memory_storage() -> MemorySagaStorage:
    """
    Create and return a fresh in-memory saga storage instance.
    
    Returns:
        MemorySagaStorage: A new MemorySagaStorage instance.
    """
    return MemorySagaStorage()


@pytest.mark.benchmark
def test_benchmark_saga_fallback_without_circuit_breaker(
    benchmark,
    saga_container_fallback: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark saga with Fallback step (no circuit breaker). Primary step runs."""

    class SagaWithFallbackNoCB(Saga[OrderContext]):
        steps = [
            Fallback(
                step=ReserveInventoryStep,
                fallback=FallbackReserveStep,
                circuit_breaker=None,
            ),
            ProcessPaymentStep,
            ShipOrderStep,
        ]

    saga = SagaWithFallbackNoCB()

    async def run() -> None:
        """
        Execute a Saga transaction using the fallback-enabled container and in-memory storage, iterating the transaction to completion.
        
        Creates an OrderContext and runs the configured saga (saga_container_fallback) with memory_storage, consuming the transaction iterator until finished.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container_fallback,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_saga_fallback_with_aiobreaker_adapter(
    benchmark,
    saga_container_fallback: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """
    Measure execution time of a saga that uses a Fallback step protected by an AioBreakerAdapter circuit breaker.
    
    This benchmark constructs a Saga whose first step is a Fallback-wrapped ReserveInventoryStep (fallback: FallbackReserveStep)
    with an AioBreakerAdapter configured with fail_max=5 and timeout_duration=60, then runs the saga to completion using the
    provided container and in-memory storage.
    """

    circuit_breaker = AioBreakerAdapter(fail_max=5, timeout_duration=60)

    class SagaWithFallbackWithCB(Saga[OrderContext]):
        steps = [
            Fallback(
                step=ReserveInventoryStep,
                fallback=FallbackReserveStep,
                circuit_breaker=circuit_breaker,
            ),
            ProcessPaymentStep,
            ShipOrderStep,
        ]

    saga = SagaWithFallbackWithCB()

    async def run() -> None:
        """
        Execute a Saga transaction using the fallback-enabled container and in-memory storage, iterating the transaction to completion.
        
        Creates an OrderContext and runs the configured saga (saga_container_fallback) with memory_storage, consuming the transaction iterator until finished.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container_fallback,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())
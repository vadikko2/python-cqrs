"""Benchmarks for Saga with Fallback (dataclass DCResponse)."""

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
        Initialize the fallback step and prepare an empty list to record emitted events.
        
        The instance will use the `_events` list to store Event objects; access should be via the public `events` property which returns a copy.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Provide a shallow copy of the recorded events.
        
        Returns:
            list[Event]: A shallow copy of the internal events list.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """
        Create a fallback inventory reservation for the given order and return it as a saga step result.
        
        Parameters:
            context (OrderContext): The saga's order context used to derive the fallback inventory identifier.
        
        Returns:
            SagaStepResult[OrderContext, ReserveInventoryResponse]: A step result containing a `ReserveInventoryResponse` whose `inventory_id` is derived from the order and whose `reserved` flag is `True`.
        """
        response = ReserveInventoryResponse(
            inventory_id=f"fallback_inv_{context.order_id}",
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        No-op compensation for the reserve fallback step.
        
        This method intentionally performs no action when compensating the fallback reserve step.
        
        Parameters:
            context (OrderContext): The saga's order context for which compensation would run.
        """
        pass


@pytest.fixture
def saga_container_fallback() -> SagaContainer:
    """
    Create a SagaContainer pre-registered with reserve, fallback, payment, and shipping step handlers.
    
    Returns:
        SagaContainer: A container with ReserveInventoryStep, FallbackReserveStep, ProcessPaymentStep, and ShipOrderStep registered.
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
    Create a fresh in-memory saga storage for tests.
    
    Returns:
        MemorySagaStorage: A new, empty MemorySagaStorage instance.
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
        Execute a complete saga transaction for a sample OrderContext using the configured saga container and memory storage.
        
        Creates an OrderContext with order_id "ord_1", user_id "user_1", and amount 100.0, opens a transaction via the module-level `saga` with `saga_container_fallback` and `memory_storage`, and iterates the transaction to completion.
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
    """Benchmark saga with Fallback step and AioBreakerAdapter circuit breaker."""

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
        Execute a complete saga transaction for a sample OrderContext using the configured saga container and memory storage.
        
        Creates an OrderContext with order_id "ord_1", user_id "user_1", and amount 100.0, opens a transaction via the module-level `saga` with `saga_container_fallback` and `memory_storage`, and iterates the transaction to completion.
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
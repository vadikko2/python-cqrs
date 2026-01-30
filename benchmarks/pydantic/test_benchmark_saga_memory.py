"""Benchmarks for Saga with memory storage (Pydantic Response)."""

import dataclasses
import typing

import pytest
from cqrs.events.event import Event
from cqrs.response import Response
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    user_id: str
    amount: float


class ReserveInventoryResponse(Response):
    inventory_id: str
    reserved: bool


class ProcessPaymentResponse(Response):
    payment_id: str
    charged: bool


class ShipOrderResponse(Response):
    shipment_id: str
    shipped: bool


class ReserveInventoryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    def __init__(self) -> None:
        """
        Initialize the step handler and prepare an empty list to record emitted events.
        
        The instance attribute `_events` is created as an empty list used to store Event objects produced by the handler.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Return a shallow copy of the recorded events.
        
        Returns:
            list[Event]: A list of recorded Event objects. Modifying the returned list does not affect the handler's internal events.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """
        Create a ReserveInventoryResponse indicating inventory has been reserved for the provided order context.
        
        Parameters:
            context (OrderContext): The saga context containing the order identifiers used to associate the reservation.
        
        Returns:
            SagaStepResult[OrderContext, ReserveInventoryResponse]: A step result carrying a ReserveInventoryResponse with an `inventory_id` tied to the order and `reserved` set to `True`.
        """
        response = ReserveInventoryResponse(
            inventory_id=f"inv_{context.order_id}",
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        Perform compensation for the step; this implementation performs no action.
        
        Parameters:
            context (OrderContext): The saga context for which compensation would run.
        """
        pass


class ProcessPaymentStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
    def __init__(self) -> None:
        """
        Initialize the step handler and prepare an empty list to record emitted events.
        
        The instance attribute `_events` is created as an empty list used to store Event objects produced by the handler.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Return a shallow copy of the recorded events.
        
        Returns:
            list[Event]: A list of recorded Event objects. Modifying the returned list does not affect the handler's internal events.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        """
        Create a ProcessPaymentResponse for the given order and wrap it in a SagaStepResult.
        
        Parameters:
            context (OrderContext): The saga's order context containing identifiers and amount.
        
        Returns:
            SagaStepResult[OrderContext, ProcessPaymentResponse]: Step result containing a `ProcessPaymentResponse` with `payment_id` derived from `context.order_id` and `charged` set to `True`.
        """
        response = ProcessPaymentResponse(
            payment_id=f"pay_{context.order_id}",
            charged=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        Perform compensation for the step; this implementation performs no action.
        
        Parameters:
            context (OrderContext): The saga context for which compensation would run.
        """
        pass


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    def __init__(self) -> None:
        """
        Initialize the step handler and prepare an empty list to record emitted events.
        
        The instance attribute `_events` is created as an empty list used to store Event objects produced by the handler.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Return a shallow copy of the recorded events.
        
        Returns:
            list[Event]: A list of recorded Event objects. Modifying the returned list does not affect the handler's internal events.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        """
        Create a ShipOrderResponse indicating the order has been shipped and wrap it in a SagaStepResult.
        
        Returns:
            SagaStepResult[OrderContext, ShipOrderResponse]: A step result containing a ShipOrderResponse with shipment details.
        """
        response = ShipOrderResponse(
            shipment_id=f"ship_{context.order_id}",
            shipped=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        Perform compensation for the step; this implementation performs no action.
        
        Parameters:
            context (OrderContext): The saga context for which compensation would run.
        """
        pass


class SagaContainer:
    """Simple container that resolves saga step handlers."""

    def __init__(self) -> None:
        """
        Initialize the SagaContainer with no registered handlers and no attached external container.
        
        Sets up internal state:
        - _handlers: an empty mapping from handler types to SagaStepHandler instances.
        - _external_container: the attached external container reference, initialized to `None`.
        """
        self._handlers: dict[type, SagaStepHandler] = {}
        self._external_container: typing.Any = None

    def register(self, handler_type: type, handler: SagaStepHandler) -> None:
        """
        Register a saga step handler instance under the given handler type.
        
        Parameters:
            handler_type (type): The key type used to resolve the handler (typically the handler class).
            handler (SagaStepHandler): The handler instance to store and return when resolving `handler_type`.
        """
        self._handlers[handler_type] = handler

    @property
    def external_container(self) -> typing.Any:
        """
        Get the attached external dependency container.
        
        Returns:
            typing.Any: The external container instance if one has been attached, otherwise `None`.
        """
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        """
        Attach an external dependency container to the SagaContainer.
        
        Parameters:
            container (typing.Any): External container used to resolve or provide dependencies for saga step handlers.
        """
        self._external_container = container

    async def resolve(self, type_: type) -> typing.Any:
        """
        Resolve a saga step handler for the given handler class type, instantiating and caching it if necessary.
        
        Parameters:
            type_ (type): The handler class to resolve.
        
        Returns:
            typing.Any: The handler instance associated with `type_`.
        """
        if type_ not in self._handlers:
            self._handlers[type_] = type_()
        return self._handlers[type_]


@pytest.fixture
def saga_container() -> SagaContainer:
    """
    Create and return a SagaContainer pre-configured with the benchmark step handlers.
    
    Registers instances of ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep on the returned container.
    
    Returns:
        SagaContainer: A container with ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep registered.
    """
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def memory_storage() -> MemorySagaStorage:
    """
    Create a fresh in-memory saga storage instance for use in tests.
    
    Returns:
        MemorySagaStorage: A new MemorySagaStorage instance.
    """
    return MemorySagaStorage()


@pytest.fixture
def saga_with_memory_storage(
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
) -> Saga[OrderContext]:
    """
    Create an OrderSaga configured with the ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep.
    
    Returns:
        Saga[OrderContext]: an OrderSaga instance with the three steps defined.
    """
    class OrderSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep, ProcessPaymentStep, ShipOrderStep]

    return OrderSaga()


@pytest.mark.benchmark
def test_benchmark_saga_memory_full_transaction(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark full saga transaction with memory storage (3 steps)."""

    async def run() -> None:
        """
        Execute a full three-step saga transaction using the configured fixtures.
        
        Creates an OrderContext and opens a transaction on `saga_with_memory_storage` using `saga_container` and `memory_storage`, iterating the transaction to completion.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga_with_memory_storage.transaction(
            context=context,
            container=saga_container,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_saga_memory_single_step(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """
    Measure performance of a single-step saga using in-memory storage.
    
    Runs a SingleStepSaga (ReserveInventoryStep) with an OrderContext and iterates the transaction to completion.
    """

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()

    async def run() -> None:
        """
        Execute a full saga transaction for a sample order using the provided container and memory storage.
        
        Creates an OrderContext with a fixed order_id, user_id, and amount, opens the saga transaction using the given container and memory storage, and iterates the transaction to completion without performing extra actions.
        """
        context = OrderContext(order_id="ord_1", user_id="user_1", amount=100.0)
        async with saga.transaction(
            context=context,
            container=saga_container,
            storage=memory_storage,
        ) as transaction:
            async for _ in transaction:
                pass

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_saga_memory_ten_transactions(
    benchmark,
    saga_with_memory_storage: Saga[OrderContext],
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
):
    """Benchmark 10 saga transactions in sequence (memory storage)."""

    async def run() -> None:
        """
        Execute ten sequential saga transactions, each using a fresh in-memory storage and a new OrderContext.
        
        Each iteration:
        - creates a new MemorySagaStorage instance,
        - constructs an OrderContext with distinct order_id, user_id, and amount,
        - runs the saga transaction using the provided saga_with_memory_storage and saga_container fixtures and iterates the transaction to completion.
        """
        for i in range(10):
            storage = MemorySagaStorage()
            context = OrderContext(
                order_id=f"ord_{i}",
                user_id=f"user_{i}",
                amount=100.0 + i,
            )
            async with saga_with_memory_storage.transaction(
                context=context,
                container=saga_container,
                storage=storage,
            ) as transaction:
                async for _ in transaction:
                    pass

    benchmark(lambda: run())
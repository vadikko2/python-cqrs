"""Benchmarks for Saga with memory storage (dataclass DCResponse)."""

import dataclasses
import typing

import pytest
from cqrs.events.event import Event
from cqrs.response import DCResponse
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage


@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    user_id: str
    amount: float


@dataclasses.dataclass
class ReserveInventoryResponse(DCResponse):
    inventory_id: str
    reserved: bool


@dataclasses.dataclass
class ProcessPaymentResponse(DCResponse):
    payment_id: str
    charged: bool


@dataclasses.dataclass
class ShipOrderResponse(DCResponse):
    shipment_id: str
    shipped: bool


class ReserveInventoryStep(SagaStepHandler[OrderContext, ReserveInventoryResponse]):
    def __init__(self) -> None:
        """
        Initialize the step handler and prepare an internal list for recorded events.
        
        Creates an empty `_events` list used to store Event objects emitted by the step.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Get a shallow copy of the step's recorded events.
        
        Returns:
            list[Event]: A list of recorded Event objects; modifying the returned list does not change the internal events stored on the instance.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ReserveInventoryResponse]:
        """
        Create a ReserveInventoryResponse for the given order and return a SagaStepResult containing it.
        
        @returns: SagaStepResult[OrderContext, ReserveInventoryResponse] containing a ReserveInventoryResponse whose `inventory_id` is derived from `context.order_id` and `reserved` is `True`.
        """
        response = ReserveInventoryResponse(
            inventory_id=f"inv_{context.order_id}",
            reserved=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        Performs compensation for this step; in this implementation no action is taken.
        
        Parameters:
            context (OrderContext): The saga context for which compensation would run (unused).
        """
        pass


class ProcessPaymentStep(SagaStepHandler[OrderContext, ProcessPaymentResponse]):
    def __init__(self) -> None:
        """
        Initialize the step handler and prepare an internal list for recorded events.
        
        Creates an empty `_events` list used to store Event objects emitted by the step.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Get a shallow copy of the step's recorded events.
        
        Returns:
            list[Event]: A list of recorded Event objects; modifying the returned list does not change the internal events stored on the instance.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ProcessPaymentResponse]:
        """
        Processes payment for the provided order context.
        
        Parameters:
            context (OrderContext): The saga context containing order details used to process payment.
        
        Returns:
            SagaStepResult[OrderContext, ProcessPaymentResponse]: A step result containing a ProcessPaymentResponse with the generated `payment_id` and `charged` set to `True`.
        """
        response = ProcessPaymentResponse(
            payment_id=f"pay_{context.order_id}",
            charged=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        Performs compensation for this step; in this implementation no action is taken.
        
        Parameters:
            context (OrderContext): The saga context for which compensation would run (unused).
        """
        pass


class ShipOrderStep(SagaStepHandler[OrderContext, ShipOrderResponse]):
    def __init__(self) -> None:
        """
        Initialize the step handler and prepare an internal list for recorded events.
        
        Creates an empty `_events` list used to store Event objects emitted by the step.
        """
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        """
        Get a shallow copy of the step's recorded events.
        
        Returns:
            list[Event]: A list of recorded Event objects; modifying the returned list does not change the internal events stored on the instance.
        """
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, ShipOrderResponse]:
        """
        Create a ShipOrderResponse for the given order and return a SagaStepResult that wraps it.
        
        Returns:
            SagaStepResult[OrderContext, ShipOrderResponse]: A step result containing a ShipOrderResponse whose `shipment_id` is derived from `context.order_id` and whose `shipped` flag is `True`.
        """
        response = ShipOrderResponse(
            shipment_id=f"ship_{context.order_id}",
            shipped=True,
        )
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        """
        Performs compensation for this step; in this implementation no action is taken.
        
        Parameters:
            context (OrderContext): The saga context for which compensation would run (unused).
        """
        pass


class SagaContainer:
    """Simple container that resolves saga step handlers."""

    def __init__(self) -> None:
        """
        Initialize the SagaContainer with an empty handler registry and no attached external container.
        
        The container maintains a mapping from handler types to their SagaStepHandler instances in `_handlers` and stores an optional external dependency resolver in `_external_container`, initially `None`.
        """
        self._handlers: dict[type, SagaStepHandler] = {}
        self._external_container: typing.Any = None

    def register(self, handler_type: type, handler: SagaStepHandler) -> None:
        """
        Register a saga step handler instance for a given handler type.
        
        Parameters:
            handler_type (type): The step handler class or identifier used as the key for resolution.
            handler (SagaStepHandler): The handler instance to associate with `handler_type`. If a handler is already registered for the type, it will be replaced.
        """
        self._handlers[handler_type] = handler

    @property
    def external_container(self) -> typing.Any:
        """
        Get the external dependency container attached to this SagaContainer.
        
        Returns:
            The attached external container, or `None` if no container has been attached.
        """
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        """
        Attach an external dependency container to be used when resolving saga step handlers.
        
        Parameters:
            container (typing.Any): An external container or service locator that SagaContainer.resolve may consult to create or retrieve handler instances.
        """
        self._external_container = container

    async def resolve(self, type_: type) -> typing.Any:
        """
        Resolve or instantiate a saga step handler for the given handler class.
        
        Parameters:
            type_ (type): The handler class to resolve; must be constructible without arguments.
        
        Returns:
            typing.Any: An instance of the requested handler class (existing or newly created).
        """
        if type_ not in self._handlers:
            self._handlers[type_] = type_()
        return self._handlers[type_]


@pytest.fixture
def saga_container() -> SagaContainer:
    """
    Create a SagaContainer pre-registered with the benchmark saga step handlers.
    
    Registers ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep instances on the container and returns it.
    
    Returns:
        SagaContainer: A container with ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep already registered.
    """
    container = SagaContainer()
    container.register(ReserveInventoryStep, ReserveInventoryStep())
    container.register(ProcessPaymentStep, ProcessPaymentStep())
    container.register(ShipOrderStep, ShipOrderStep())
    return container


@pytest.fixture
def memory_storage() -> MemorySagaStorage:
    """
    Create a new MemorySagaStorage instance for use in tests.
    
    Returns:
        MemorySagaStorage: In-memory storage instance for persisting saga state.
    """
    return MemorySagaStorage()


@pytest.fixture
def saga_with_memory_storage(
    saga_container: SagaContainer,
    memory_storage: MemorySagaStorage,
) -> Saga[OrderContext]:
    """
    Create and return an OrderSaga configured with ReserveInventory, ProcessPayment, and ShipOrder steps.
    
    Parameters:
        saga_container (SagaContainer): Registered saga container; ensures step handler types are available.
        memory_storage (MemorySagaStorage): Memory-backed saga storage; provided to align fixture lifecycle.
    
    Returns:
        OrderSaga: A new saga instance whose steps are ReserveInventoryStep, ProcessPaymentStep, and ShipOrderStep.
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
        Execute a full three-step saga transaction using the fixture-provided saga, container, and memory storage.
        
        Creates an OrderContext with order_id "ord_1", user_id "user_1", and amount 100.0, runs the saga transaction, and iterates through all step results without performing additional actions.
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
    Benchmark running a single-step Saga transaction using memory-backed storage.
    
    This test measures the performance of executing a Saga composed of a single ReserveInventoryStep by invoking the Saga transaction with the provided container and memory storage via the benchmark fixture.
    """

    class SingleStepSaga(Saga[OrderContext]):
        steps = [ReserveInventoryStep]

    saga = SingleStepSaga()

    async def run() -> None:
        """
        Run a full three-step saga transaction for a sample OrderContext, iterating through each step without performing additional actions.
        
        This function creates an OrderContext (order_id "ord_1", user_id "user_1", amount 100.0) and executes a saga.transaction using the provided container and memory storage, consuming each yielded step result.
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
        Run ten sequential saga transactions using a fresh MemorySagaStorage and a distinct OrderContext for each iteration.
        
        Each iteration creates a new MemorySagaStorage and OrderContext (order_id and user_id suffixed with the iteration index, amount incremented by the index), opens a transaction via the surrounding saga fixture and container, and iterates through the transaction steps.
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
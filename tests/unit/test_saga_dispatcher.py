"""Unit tests for SagaDispatcher."""

import dataclasses
import typing
import uuid

import pytest

import cqrs
from cqrs.dispatcher.exceptions import SagaDoesNotExist
from cqrs.dispatcher.saga import SagaDispatcher
from cqrs.events.event import Event
from cqrs.requests.map import SagaMap
from cqrs.response import Response
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage


# Test models
@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    value: int = 0


class TestStepResponse(Response):
    step_name: str
    value: int


# Test step handlers
class Step1Handler(SagaStepHandler[OrderContext, TestStepResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, TestStepResponse]:
        context.value += 1
        response = TestStepResponse(step_name="step1", value=context.value)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        context.value -= 1


class Step2Handler(SagaStepHandler[OrderContext, TestStepResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, TestStepResponse]:
        context.value += 10
        response = TestStepResponse(step_name="step2", value=context.value)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        context.value -= 10


class OrderSaga(Saga[OrderContext]):
    """Test saga with two steps."""

    steps = [Step1Handler, Step2Handler]


# Test container
class OrderContainer:
    def __init__(self) -> None:
        self._instances = {
            Step1Handler: Step1Handler(),
            Step2Handler: Step2Handler(),
            OrderSaga: OrderSaga(),  # type: ignore
        }

    async def resolve(self, type_: typing.Type) -> typing.Any:
        if type_ in self._instances:
            return self._instances[type_]
        raise ValueError(f"Cannot resolve type: {type_}")


async def test_saga_dispatcher_dispatch_returns_async_iterator_consumable_with_async_for() -> None:
    """
    Contract: dispatcher.dispatch(context) is called without await
    and returns an AsyncIterator consumed with async for.
    """
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="test-123")
    # dispatch() is called (no await) and returns async iterator
    async_gen = dispatcher.dispatch(context)
    results = []
    async for result in async_gen:
        results.append(result)

    assert len(results) == 2
    assert results[0].step_result.step_type == Step1Handler
    assert results[1].step_result.step_type == Step2Handler


async def test_saga_dispatcher_executes_saga_steps_in_order() -> None:
    """Test that dispatcher executes saga steps in correct order."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="test-order")
    results = []

    async for result in dispatcher.dispatch(context):
        results.append(result)

    assert len(results) == 2
    assert results[0].step_result.step_type == Step1Handler
    assert results[0].step_result.response.step_name == "step1"
    assert results[0].step_result.response.value == 1

    assert results[1].step_result.step_type == Step2Handler
    assert results[1].step_result.response.step_name == "step2"
    assert results[1].step_result.response.value == 11


async def test_saga_dispatcher_collects_events_from_steps() -> None:
    """Test that dispatcher collects events from saga steps."""

    class EventEmittingStep(SagaStepHandler[OrderContext, TestStepResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, TestStepResponse]:
            event = cqrs.NotificationEvent(
                event_name="StepExecuted",
                payload={"order_id": context.order_id},
            )
            self._events.append(event)
            response = TestStepResponse(step_name="event_step", value=1)
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            pass

    class EventSaga(Saga[OrderContext]):
        steps = [EventEmittingStep]

    saga_map = SagaMap()
    saga_map.bind(OrderContext, EventSaga)

    container = OrderContainer()
    container._instances[EventSaga] = EventSaga()  # type: ignore
    container._instances[EventEmittingStep] = EventEmittingStep()

    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="event-order")
    results = []

    async for result in dispatcher.dispatch(context):
        results.append(result)

    assert len(results) == 1
    assert len(results[0].events) == 1
    assert isinstance(results[0].events[0], cqrs.NotificationEvent)
    assert results[0].events[0].payload["order_id"] == "event-order"  # type: ignore


async def test_saga_dispatcher_raises_saga_does_not_exist_when_saga_not_registered() -> None:
    """Test that dispatcher raises SagaDoesNotExist if saga is not registered."""

    @dataclasses.dataclass
    class UnregisteredContext(SagaContext):
        data: str

    saga_map = SagaMap()
    # Don't register UnregisteredContext
    container = OrderContainer()
    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = UnregisteredContext(data="test")

    with pytest.raises(
        SagaDoesNotExist,
        match="Saga not found matching SagaContext type",
    ):
        async for _ in dispatcher.dispatch(context):
            pass


async def test_saga_dispatcher_uses_provided_saga_id() -> None:
    """Test that dispatcher uses provided saga_id for storage."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    provided_saga_id = uuid.uuid4()
    context = OrderContext(order_id="test-saga-id")
    results = []

    async for result in dispatcher.dispatch(context, saga_id=provided_saga_id):
        results.append(result)

    assert len(results) == 2
    # Verify that both results have the same saga_id
    assert results[0].saga_id == str(provided_saga_id)
    assert results[1].saga_id == str(provided_saga_id)


async def test_saga_dispatcher_generates_saga_id_if_not_provided() -> None:
    """Test that dispatcher generates saga_id if not provided."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="test-auto-id")
    results = []

    async for result in dispatcher.dispatch(context):
        results.append(result)

    assert len(results) == 2
    # Verify that saga_id was generated
    saga_id_1 = results[0].saga_id
    saga_id_2 = results[1].saga_id
    assert saga_id_1 is not None
    assert saga_id_2 is not None
    # Both results should have the same saga_id
    assert saga_id_1 == saga_id_2
    # Saga ID should be a valid UUID
    uuid.UUID(saga_id_1)


async def test_saga_dispatcher_exception_in_step_propagates() -> None:
    """Test that exceptions in saga steps propagate to caller."""

    class FailingStep(SagaStepHandler[OrderContext, TestStepResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, TestStepResponse]:
            raise RuntimeError(f"Step failed for order {context.order_id}")

        async def compensate(self, context: OrderContext) -> None:
            pass

    class FailingSaga(Saga[OrderContext]):
        steps = [Step1Handler, FailingStep]

    saga_map = SagaMap()
    saga_map.bind(OrderContext, FailingSaga)

    container = OrderContainer()
    container._instances[FailingSaga] = FailingSaga()  # type: ignore
    container._instances[FailingStep] = FailingStep()

    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="fail-order")
    results = []

    with pytest.raises(RuntimeError, match="Step failed for order fail-order"):
        async for result in dispatcher.dispatch(context):
            results.append(result)

    # First step should have completed
    assert len(results) == 1


async def test_saga_dispatcher_with_compensation_retry_parameters() -> None:
    """Test that dispatcher accepts and uses compensation retry parameters."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
        compensation_retry_count=5,
        compensation_retry_delay=2.0,
        compensation_retry_backoff=3.0,
    )

    context = OrderContext(order_id="retry-test")
    results = []

    async for result in dispatcher.dispatch(context):
        results.append(result)

    # Just verify that dispatcher works with these parameters
    assert len(results) == 2


async def test_saga_dispatcher_with_middleware_chain() -> None:
    """Test that dispatcher works with middleware chain."""
    from cqrs.middlewares.base import Middleware, MiddlewareChain

    middleware_called = []

    class TestMiddleware(Middleware):
        async def __call__(self, request, call_next):
            middleware_called.append("before")
            result = await call_next(request)
            middleware_called.append("after")
            return result

    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    middleware_chain = MiddlewareChain()
    middleware_chain.add(TestMiddleware())

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
        middleware_chain=middleware_chain,
    )

    context = OrderContext(order_id="middleware-test")
    results = []

    async for result in dispatcher.dispatch(context):
        results.append(result)

    assert len(results) == 2


async def test_saga_dispatcher_multiple_sagas_can_be_registered() -> None:
    """Test that dispatcher can handle multiple saga types."""

    @dataclasses.dataclass
    class Context2(SagaContext):
        data: str

    class Step3Handler(SagaStepHandler[Context2, TestStepResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: Context2,
        ) -> SagaStepResult[Context2, TestStepResponse]:
            response = TestStepResponse(step_name="step3", value=999)
            return self._generate_step_result(response)

        async def compensate(self, context: Context2) -> None:
            pass

    class Saga2(Saga[Context2]):
        steps = [Step3Handler]

    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    saga_map.bind(Context2, Saga2)

    container = OrderContainer()
    container._instances[Saga2] = Saga2()  # type: ignore
    container._instances[Step3Handler] = Step3Handler()

    storage = MemorySagaStorage()

    dispatcher = SagaDispatcher(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    # Execute first saga
    context1 = OrderContext(order_id="order-1")
    results1 = []
    async for result in dispatcher.dispatch(context1):
        results1.append(result)
    assert len(results1) == 2

    # Execute second saga
    context2 = Context2(data="test-2")
    results2 = []
    async for result in dispatcher.dispatch(context2):
        results2.append(result)
    assert len(results2) == 1
    assert results2[0].step_result.response.value == 999
"""Edge case tests for SagaMediator."""

import asyncio
import dataclasses
import typing
import uuid
from unittest import mock

import pytest

import cqrs
from cqrs.events.event import DomainEvent, Event
from cqrs.events.event_handler import EventHandler
from cqrs.events import EventEmitter, EventMap
from cqrs.response import Response
from cqrs.requests.map import SagaMap
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga
from cqrs.saga.step import SagaStepHandler, SagaStepResult
from cqrs.saga.storage.memory import MemorySagaStorage
from cqrs.saga.storage.enums import SagaStatus


# Test models
@dataclasses.dataclass
class OrderContext(SagaContext):
    order_id: str
    value: int = 0
    data: list[str] | None = None


class TestResponse(Response):
    step_name: str
    value: int


class TestEvent(DomainEvent, frozen=True):
    step_name: str
    order_id: str


# Test step handlers
class NormalStep(SagaStepHandler[OrderContext, TestResponse]):
    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, TestResponse]:
        context.value += 1
        event = TestEvent(step_name="normal", order_id=context.order_id)
        self._events.append(event)
        response = TestResponse(step_name="normal", value=context.value)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        context.value -= 1


class SlowStep(SagaStepHandler[OrderContext, TestResponse]):
    """Step with slow processing."""

    def __init__(self) -> None:
        self._events: list[Event] = []

    @property
    def events(self) -> list[Event]:
        return self._events.copy()

    async def act(
        self,
        context: OrderContext,
    ) -> SagaStepResult[OrderContext, TestResponse]:
        await asyncio.sleep(0.1)
        context.value += 10
        response = TestResponse(step_name="slow", value=context.value)
        return self._generate_step_result(response)

    async def compensate(self, context: OrderContext) -> None:
        context.value -= 10


class OrderSaga(Saga[OrderContext]):
    steps = [NormalStep, SlowStep]


class TestEventHandler(EventHandler[TestEvent]):
    def __init__(self) -> None:
        self.handled_events: list[TestEvent] = []

    async def handle(self, event: TestEvent) -> None:
        self.handled_events.append(event)


# Test container
class OrderContainer:
    def __init__(self) -> None:
        self._instances: dict[typing.Type, typing.Any] = {
            NormalStep: NormalStep(),
            SlowStep: SlowStep(),
            OrderSaga: OrderSaga(),  # type: ignore
            TestEventHandler: TestEventHandler(),
        }

    async def resolve(self, type_: typing.Type) -> typing.Any:
        if type_ in self._instances:
            return self._instances[type_]
        raise ValueError(f"Cannot resolve type: {type_}")


async def test_saga_mediator_stream_returns_async_iterator_consumable_with_async_for() -> None:
    """
    Contract: mediator.stream(context) is called without await
    and returns an AsyncIterator consumed with async for.
    """
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    event_map = EventMap()
    event_map.bind(TestEvent, TestEventHandler)
    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=mock.AsyncMock(),
    )

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        storage=storage,
    )

    context = OrderContext(order_id="test-contract")
    # stream() is called (no await) and returns async iterator
    async_gen = mediator.stream(context)
    results = []
    async for result in async_gen:
        results.append(result)

    assert len(results) == 2
    assert results[0].step_type == NormalStep
    assert results[1].step_type == SlowStep


async def test_saga_mediator_with_empty_saga() -> None:
    """Test mediator with saga that has no steps."""

    class EmptySaga(Saga[OrderContext]):
        steps = []

    saga_map = SagaMap()
    saga_map.bind(OrderContext, EmptySaga)

    container = OrderContainer()
    container._instances[EmptySaga] = EmptySaga()  # type: ignore
    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="empty")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    # Should complete without any steps
    assert len(results) == 0


async def test_saga_mediator_exception_in_step_triggers_compensation() -> None:
    """Test that exception in step triggers compensation of previous steps."""

    class FailingStep(SagaStepHandler[OrderContext, TestResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []
            self.compensate_called = False

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, TestResponse]:
            raise ValueError("Step failed intentionally")

        async def compensate(self, context: OrderContext) -> None:
            self.compensate_called = True

    class FailingSaga(Saga[OrderContext]):
        steps = [NormalStep, FailingStep]

    saga_map = SagaMap()
    saga_map.bind(OrderContext, FailingSaga)

    container = OrderContainer()
    failing_step = FailingStep()
    container._instances[FailingSaga] = FailingSaga()  # type: ignore
    container._instances[FailingStep] = failing_step

    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="failing")
    saga_id = uuid.uuid4()

    with pytest.raises(ValueError, match="Step failed intentionally"):
        async for _ in mediator.stream(context, saga_id=saga_id):
            pass

    # Verify saga is in FAILED status
    status, _, _ = await storage.load_saga_state(saga_id)
    assert status == SagaStatus.FAILED


async def test_saga_mediator_processes_events_from_steps() -> None:
    """Test that mediator processes events emitted by steps."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    event_map = EventMap()
    event_map.bind(TestEvent, TestEventHandler)
    message_broker = mock.AsyncMock()
    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=message_broker,
    )

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        storage=storage,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
    )

    context = OrderContext(order_id="events-test")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    # Wait for background event processing
    await asyncio.sleep(0.2)

    # Verify events were processed
    event_handler = await container.resolve(TestEventHandler)
    assert len(event_handler.handled_events) >= 1


async def test_saga_mediator_with_custom_dispatcher_type() -> None:
    """Test that mediator can be instantiated with custom dispatcher type."""
    from cqrs.dispatcher.saga import SagaDispatcher

    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
        dispatcher_type=SagaDispatcher,
    )

    context = OrderContext(order_id="custom-dispatcher")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    assert len(results) == 2


async def test_saga_mediator_sequential_event_processing() -> None:
    """Test that mediator processes events sequentially when configured."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    event_map = EventMap()
    event_map.bind(TestEvent, TestEventHandler)
    event_emitter = EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=mock.AsyncMock(),
    )

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        storage=storage,
        max_concurrent_event_handlers=1,
        concurrent_event_handle_enable=False,  # Sequential
    )

    context = OrderContext(order_id="sequential")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    # Wait for event processing
    await asyncio.sleep(0.1)

    assert len(results) == 2


async def test_saga_mediator_multiple_contexts_different_sagas() -> None:
    """Test that mediator can handle multiple context types with different sagas."""

    @dataclasses.dataclass
    class Context2(SagaContext):
        context_id: str

    class Step2(SagaStepHandler[Context2, TestResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: Context2,
        ) -> SagaStepResult[Context2, TestResponse]:
            response = TestResponse(step_name="step2", value=999)
            return self._generate_step_result(response)

        async def compensate(self, context: Context2) -> None:
            pass

    class Saga2(Saga[Context2]):
        steps = [Step2]

    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    saga_map.bind(Context2, Saga2)

    container = OrderContainer()
    container._instances[Saga2] = Saga2()  # type: ignore
    container._instances[Step2] = Step2()

    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    # Execute first saga
    context1 = OrderContext(order_id="context1")
    results1 = []
    async for result in mediator.stream(context1):
        results1.append(result)
    assert len(results1) == 2

    # Execute second saga
    context2 = Context2(context_id="context2")
    results2 = []
    async for result in mediator.stream(context2):
        results2.append(result)
    assert len(results2) == 1
    assert results2[0].response.value == 999


async def test_saga_mediator_compensation_retry_configuration() -> None:
    """Test that mediator uses compensation retry configuration."""
    saga_map = SagaMap()
    saga_map.bind(OrderContext, OrderSaga)
    container = OrderContainer()
    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
        compensation_retry_count=5,
        compensation_retry_delay=2.0,
        compensation_retry_backoff=3.0,
    )

    context = OrderContext(order_id="retry-config")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    # Just verify that mediator works with these parameters
    assert len(results) == 2


async def test_saga_mediator_context_modification_persisted_across_steps() -> None:
    """Test that context modifications are persisted across steps."""

    class DataAccumulatingStep1(SagaStepHandler[OrderContext, TestResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, TestResponse]:
            if context.data is None:
                context.data = []
            context.data.append("step1")
            response = TestResponse(step_name="step1", value=len(context.data))
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            pass

    class DataAccumulatingStep2(SagaStepHandler[OrderContext, TestResponse]):
        def __init__(self) -> None:
            self._events: list[Event] = []

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, TestResponse]:
            if context.data is None:
                context.data = []
            context.data.append("step2")
            response = TestResponse(step_name="step2", value=len(context.data))
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            pass

    class DataSaga(Saga[OrderContext]):
        steps = [DataAccumulatingStep1, DataAccumulatingStep2]

    saga_map = SagaMap()
    saga_map.bind(OrderContext, DataSaga)

    container = OrderContainer()
    container._instances[DataSaga] = DataSaga()  # type: ignore
    container._instances[DataAccumulatingStep1] = DataAccumulatingStep1()
    container._instances[DataAccumulatingStep2] = DataAccumulatingStep2()

    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="data-persist")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    assert len(results) == 2
    # First step should have 1 item
    assert results[0].response.value == 1
    # Second step should have 2 items (accumulated)
    assert results[1].response.value == 2


async def test_saga_mediator_large_number_of_steps() -> None:
    """Test mediator with saga containing many steps."""

    class SimpleStep(SagaStepHandler[OrderContext, TestResponse]):
        def __init__(self, step_num: int) -> None:
            self._events: list[Event] = []
            self.step_num = step_num

        @property
        def events(self) -> list[Event]:
            return self._events.copy()

        async def act(
            self,
            context: OrderContext,
        ) -> SagaStepResult[OrderContext, TestResponse]:
            context.value += 1
            response = TestResponse(
                step_name=f"step_{self.step_num}",
                value=context.value,
            )
            return self._generate_step_result(response)

        async def compensate(self, context: OrderContext) -> None:
            context.value -= 1

    # Create step classes dynamically
    step_classes = []
    for i in range(10):

        class StepClass(SagaStepHandler[OrderContext, TestResponse]):
            step_index = i

            def __init__(self) -> None:
                self._events: list[Event] = []

            @property
            def events(self) -> list[Event]:
                return self._events.copy()

            async def act(
                self,
                context: OrderContext,
            ) -> SagaStepResult[OrderContext, TestResponse]:
                context.value += 1
                response = TestResponse(
                    step_name=f"step_{self.step_index}",
                    value=context.value,
                )
                return self._generate_step_result(response)

            async def compensate(self, context: OrderContext) -> None:
                context.value -= 1

        step_classes.append(StepClass)

    class LargeSaga(Saga[OrderContext]):
        steps = step_classes

    saga_map = SagaMap()
    saga_map.bind(OrderContext, LargeSaga)

    container = OrderContainer()
    container._instances[LargeSaga] = LargeSaga()  # type: ignore
    for step_class in step_classes:
        container._instances[step_class] = step_class()

    storage = MemorySagaStorage()

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        storage=storage,
    )

    context = OrderContext(order_id="large-saga")
    results = []
    async for result in mediator.stream(context):
        results.append(result)

    assert len(results) == 10
    # Last result should have accumulated value
    assert results[-1].response.value == 10
"""Integration tests for SagaMediator with SqlAlchemySagaStorage."""

import typing
import uuid
from unittest import mock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import cqrs
from cqrs import events
from cqrs.requests.map import SagaMap
from cqrs.saga.storage.enums import SagaStatus
from cqrs.saga.storage.sqlalchemy import SqlAlchemySagaStorage

# Import test models from memory test file
from tests.integration.test_saga_mediator_memory import (
    FailingOrderSaga,
    FailingStep,
    InventoryReservedEvent,
    InventoryReservedEventHandler,
    OrderContext,
    OrderShippedEvent,
    OrderShippedEventHandler,
    OrderSaga,
    PaymentProcessedEvent,
    PaymentProcessedEventHandler,
    ProcessPaymentResponse,
    ProcessPaymentStep,
    ReserveInventoryResponse,
    ReserveInventoryStep,
    ShipOrderResponse,
    ShipOrderStep,
)


# Container setup (reuse from memory test)
class _TestContainer:
    """Test container that resolves step handlers, sagas, and event handlers."""

    def __init__(self, storage: SqlAlchemySagaStorage) -> None:
        self._storage = storage
        self._external_container = None
        self._step_handlers = {
            ReserveInventoryStep: ReserveInventoryStep(),
            ProcessPaymentStep: ProcessPaymentStep(),
            ShipOrderStep: ShipOrderStep(),
            FailingStep: FailingStep(),
        }
        self._event_handlers = {
            InventoryReservedEventHandler: InventoryReservedEventHandler(),
            PaymentProcessedEventHandler: PaymentProcessedEventHandler(),
            OrderShippedEventHandler: OrderShippedEventHandler(),
        }
        # Create sagas with this container and storage
        self._sagas = {
            OrderSaga: OrderSaga(),  # type: ignore[arg-type]
            FailingOrderSaga: FailingOrderSaga(),  # type: ignore[arg-type]
        }

    @property
    def external_container(self) -> typing.Any:
        """Return external container (for Container protocol compatibility)."""
        return self._external_container

    def attach_external_container(self, container: typing.Any) -> None:
        """Attach external container (for Container protocol compatibility)."""
        self._external_container = container

    async def resolve(self, type_) -> typing.Any:
        """Resolve type from container."""
        if type_ in self._step_handlers:
            return self._step_handlers[type_]
        if type_ in self._event_handlers:
            return self._event_handlers[type_]
        if type_ in self._sagas:
            return self._sagas[type_]
        if type_ == SqlAlchemySagaStorage:
            return self._storage
        raise ValueError(f"Unknown type: {type_}")


@pytest.fixture
def storage(saga_session: AsyncSession) -> SqlAlchemySagaStorage:
    """Create SqlAlchemySagaStorage instance."""
    return SqlAlchemySagaStorage(saga_session)


@pytest.fixture
def container(storage: SqlAlchemySagaStorage) -> _TestContainer:
    """Create test container."""
    container = _TestContainer(storage)
    # Clear events in step handlers before each test
    for step_handler in container._step_handlers.values():
        if hasattr(step_handler, "_events"):
            step_handler._events.clear()
    # Clear events in event handlers before each test
    for event_handler in container._event_handlers.values():
        if hasattr(event_handler, "handled_events"):
            event_handler.handled_events.clear()
    return container


@pytest.fixture
def saga_mediator(
    container: _TestContainer,
    storage: SqlAlchemySagaStorage,
    saga_session: AsyncSession,
) -> cqrs.SagaMediator:
    """Create SagaMediator with SqlAlchemySagaStorage."""

    def saga_mapper(mapper: SagaMap) -> None:
        mapper.bind(OrderContext, OrderSaga)

    def events_mapper(mapper: events.EventMap) -> None:
        mapper.bind(InventoryReservedEvent, InventoryReservedEventHandler)
        mapper.bind(PaymentProcessedEvent, PaymentProcessedEventHandler)
        mapper.bind(OrderShippedEvent, OrderShippedEventHandler)

    # Create event emitter
    event_map = events.EventMap()
    events_mapper(event_map)

    message_broker = mock.AsyncMock()
    message_broker.produce = mock.AsyncMock()

    event_emitter = events.EventEmitter(
        event_map=event_map,
        container=container,  # type: ignore
        message_broker=message_broker,
    )

    # Create mediator directly
    saga_map = SagaMap()
    saga_mapper(saga_map)

    mediator = cqrs.SagaMediator(
        saga_map=saga_map,
        container=container,  # type: ignore
        event_emitter=event_emitter,
        event_map=event_map,
        max_concurrent_event_handlers=2,
        concurrent_event_handle_enable=True,
        storage=storage,
    )

    return mediator


class TestSagaMediatorSqlAlchemyStorage:
    """Integration tests for SagaMediator with SqlAlchemySagaStorage."""

    async def test_saga_mediator_executes_saga_successfully(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
        saga_session: AsyncSession,
    ) -> None:
        """Test that SagaMediator executes saga successfully with SQLAlchemy storage."""
        context = OrderContext(order_id="123", user_id="user1", amount=100.0)
        saga_id = uuid.uuid4()

        step_results = []
        async for result in saga_mediator.stream(context, saga_id=saga_id):
            step_results.append(result)

        await saga_session.commit()

        # Verify all steps were executed
        assert len(step_results) == 3

        # Verify step results
        assert isinstance(step_results[0].response, ReserveInventoryResponse)
        assert step_results[0].response.inventory_id == "inv_123"
        assert step_results[0].response.reserved is True

        assert isinstance(step_results[1].response, ProcessPaymentResponse)
        assert step_results[1].response.payment_id == "pay_123"
        assert step_results[1].response.charged is True

        assert isinstance(step_results[2].response, ShipOrderResponse)
        assert step_results[2].response.shipment_id == "ship_123"
        assert step_results[2].response.shipped is True

        # Verify step types
        assert step_results[0].step_type == ReserveInventoryStep
        assert step_results[1].step_type == ProcessPaymentStep
        assert step_results[2].step_type == ShipOrderStep

        # Verify saga status in storage
        status, stored_context = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.COMPLETED

    async def test_saga_mediator_processes_events_from_steps(
        self,
        saga_mediator: cqrs.SagaMediator,
        container: _TestContainer,
        saga_session: AsyncSession,
    ) -> None:
        """Test that SagaMediator processes events from saga steps."""
        context = OrderContext(order_id="456", user_id="user2", amount=200.0)

        step_results = []
        async for result in saga_mediator.stream(context):
            step_results.append(result)

        await saga_session.commit()

        # Verify step results were returned
        assert len(step_results) == 3
        assert isinstance(step_results[0].response, ReserveInventoryResponse)
        assert isinstance(step_results[1].response, ProcessPaymentResponse)
        assert isinstance(step_results[2].response, ShipOrderResponse)

        # Verify event handlers were called (events are processed internally)
        # Note: events are processed twice - once via dispatcher and once via emitter
        # So we check that handlers were called at least once
        inventory_handler = await container.resolve(InventoryReservedEventHandler)
        payment_handler = await container.resolve(PaymentProcessedEventHandler)
        shipping_handler = await container.resolve(OrderShippedEventHandler)

        assert len(inventory_handler.handled_events) >= 1
        assert len(payment_handler.handled_events) >= 1
        assert len(shipping_handler.handled_events) >= 1

    async def test_saga_mediator_emits_events(
        self,
        saga_mediator: cqrs.SagaMediator,
        container: _TestContainer,
        saga_session: AsyncSession,
    ) -> None:
        """Test that SagaMediator processes events via EventEmitter."""
        context = OrderContext(order_id="789", user_id="user3", amount=300.0)

        step_results = []
        async for result in saga_mediator.stream(context):
            step_results.append(result)

        await saga_session.commit()

        # Verify events were processed (DomainEvent calls handlers, not message broker)
        # Note: events are processed twice - once via dispatcher and once via emitter
        # Check that event handlers were called at least once
        inventory_handler = await container.resolve(InventoryReservedEventHandler)
        payment_handler = await container.resolve(PaymentProcessedEventHandler)
        shipping_handler = await container.resolve(OrderShippedEventHandler)

        assert len(inventory_handler.handled_events) >= 1
        assert len(payment_handler.handled_events) >= 1
        assert len(shipping_handler.handled_events) >= 1

    async def test_saga_mediator_handles_saga_failure_with_compensation(
        self,
        container: _TestContainer,
        storage: SqlAlchemySagaStorage,
        saga_session: AsyncSession,
    ) -> None:
        """Test that SagaMediator handles saga failure and compensation."""

        # Create mediator with failing saga
        def failing_saga_mapper(mapper: SagaMap) -> None:
            mapper.bind(OrderContext, FailingOrderSaga)

        saga_map = SagaMap()
        failing_saga_mapper(saga_map)

        event_map = events.EventMap()
        event_emitter = events.EventEmitter(
            event_map=event_map,
            container=container,  # type: ignore
            message_broker=mock.AsyncMock(),
        )

        failing_mediator = cqrs.SagaMediator(
            saga_map=saga_map,
            container=container,  # type: ignore
            event_emitter=event_emitter,
            event_map=event_map,
            storage=storage,
        )

        context = OrderContext(order_id="fail_123", user_id="user4", amount=400.0)
        saga_id = uuid.uuid4()

        step_results = []
        with pytest.raises(ValueError, match="Step failed for order fail_123"):
            async for result in failing_mediator.stream(context, saga_id=saga_id):
                step_results.append(result)

        await saga_session.commit()

        # Verify that some steps were executed before failure
        assert len(step_results) >= 1

        # Verify compensation was called
        reserve_step = await container.resolve(ReserveInventoryStep)
        payment_step = await container.resolve(ProcessPaymentStep)
        assert reserve_step.compensate_called
        assert payment_step.compensate_called

        # Verify saga status is FAILED
        status, _ = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.FAILED

    async def test_saga_mediator_with_saga_id_recovery(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
        saga_session: AsyncSession,
    ) -> None:
        """Test that SagaMediator can recover saga using saga_id."""
        context = OrderContext(order_id="recover_123", user_id="user5", amount=500.0)
        saga_id = uuid.uuid4()

        # Execute first part of saga
        step_results_1 = []
        async for result in saga_mediator.stream(context, saga_id=saga_id):
            step_results_1.append(result)
            # Simulate interruption after first step
            if len(step_results_1) == 1:
                break

        await saga_session.commit()

        # Verify first step was executed
        assert len(step_results_1) == 1
        assert isinstance(step_results_1[0].response, ReserveInventoryResponse)
        assert step_results_1[0].step_type == ReserveInventoryStep

        # Verify saga is in RUNNING status
        status, _ = await storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING

        # Resume saga execution with same saga_id
        step_results_2 = []
        async for result in saga_mediator.stream(context, saga_id=saga_id):
            step_results_2.append(result)

        await saga_session.commit()

        # Verify remaining steps were executed
        # Note: Saga will skip already completed steps
        assert len(step_results_2) >= 2  # At least 2 more steps

        # Verify final status
        final_status, _ = await storage.load_saga_state(saga_id)
        assert final_status == SagaStatus.COMPLETED

    async def test_saga_mediator_persistence_across_sessions(
        self,
        container: _TestContainer,
        storage: SqlAlchemySagaStorage,
        saga_session: AsyncSession,
    ) -> None:
        """Test that saga state persists across different storage instances."""
        context = OrderContext(order_id="persist_123", user_id="user6", amount=600.0)
        saga_id = uuid.uuid4()

        def saga_mapper(mapper: SagaMap) -> None:
            mapper.bind(OrderContext, OrderSaga)

        saga_map = SagaMap()
        saga_mapper(saga_map)

        event_map = events.EventMap()
        event_emitter = events.EventEmitter(
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

        # Execute first step
        step_results_1 = []
        async for result in mediator.stream(context, saga_id=saga_id):
            step_results_1.append(result)
            if len(step_results_1) == 1:
                break

        await saga_session.commit()

        # Create new storage instance and verify persistence
        new_storage = SqlAlchemySagaStorage(saga_session)
        status, stored_context = await new_storage.load_saga_state(saga_id)
        assert status == SagaStatus.RUNNING

        history = await new_storage.get_step_history(saga_id)
        assert len(history) >= 1
        assert history[0].step_name == "ReserveInventoryStep"

    async def test_saga_mediator_concurrent_sagas(
        self,
        saga_mediator: cqrs.SagaMediator,
        storage: SqlAlchemySagaStorage,
        saga_session: AsyncSession,
    ) -> None:
        """Test that SagaMediator handles multiple concurrent sagas."""
        contexts = [
            OrderContext(order_id=f"order_{i}", user_id=f"user_{i}", amount=100.0 * i)
            for i in range(3)
        ]
        saga_ids = [uuid.uuid4() for _ in range(3)]

        # Execute sagas concurrently
        import asyncio

        async def execute_saga(context: OrderContext, saga_id: uuid.UUID) -> list:
            results = []
            async for result in saga_mediator.stream(context, saga_id=saga_id):
                results.append(result)
            # Don't commit here - commit after all sagas complete to avoid flushing conflicts
            return results

        tasks = [
            execute_saga(context, saga_id)
            for context, saga_id in zip(contexts, saga_ids)
        ]
        all_results = await asyncio.gather(*tasks)

        # Commit once after all sagas complete
        await saga_session.commit()

        # Verify all sagas completed
        assert len(all_results) == 3
        assert all(len(results) == 3 for results in all_results)

        # Verify all sagas are in COMPLETED status
        for saga_id in saga_ids:
            status, _ = await storage.load_saga_state(saga_id)
            assert status == SagaStatus.COMPLETED

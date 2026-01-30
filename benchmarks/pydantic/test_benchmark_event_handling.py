"""Benchmarks for event handling performance (Pydantic Event)."""

import typing

import di
import pytest

import cqrs
from cqrs.events import bootstrap


class UserJoinedEvent(cqrs.Event, frozen=True):
    user_id: str
    meeting_id: str


class UserJoinedEventHandler(cqrs.EventHandler[UserJoinedEvent]):
    def __init__(self):
        """
        Initialize the handler and prepare storage for processed events.
        
        Attributes:
            processed_events (List[UserJoinedEvent]): List that records events handled by this handler in arrival order.
        """
        self.processed_events: typing.List[UserJoinedEvent] = []

    async def handle(self, event: UserJoinedEvent) -> None:
        """
        Record a processed UserJoinedEvent by appending it to this handler's processed_events list.
        
        Parameters:
            event (UserJoinedEvent): The event instance to record.
        """
        self.processed_events.append(event)


def events_mapper(mapper: cqrs.EventMap) -> None:
    """
    Register the event-to-handler binding for UserJoinedEvent on the provided event map.
    
    Parameters:
        mapper (cqrs.EventMap): Event map to which the UserJoinedEvent -> UserJoinedEventHandler binding will be added.
    """
    mapper.bind(UserJoinedEvent, UserJoinedEventHandler)


@pytest.fixture
def event_mediator():
    """
    Create and return an event mediator initialized with a fresh dependency-injection container and this module's event mappings.
    
    Returns:
        An event mediator instance configured to route events to the handlers provided by this module's `events_mapper`.
    """
    return bootstrap.bootstrap(
        di_container=di.Container(),
        events_mapper=events_mapper,
    )


@pytest.mark.benchmark
def test_benchmark_event_processing(benchmark, event_mediator):
    """
    Measure performance of sending a single UserJoinedEvent through the provided event mediator.
    
    Constructs a UserJoinedEvent and benchmarks the mediator's handling by invoking its send method.
    """
    event = UserJoinedEvent(user_id="user_1", meeting_id="meeting_1")

    async def run():
        """
        Send the predefined event through the provided event mediator.
        
        Awaits sending the `event` using the enclosing scope's `event_mediator`.
        
        Returns:
            None
        """
        await event_mediator.send(event)

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_multiple_events(benchmark, event_mediator):
    """
    Benchmark sending ten UserJoinedEvent instances sequentially through the provided event mediator.
    
    Measures the time to dispatch and handle a sequence of 10 UserJoinedEvent messages sent one after another via the event_mediator.
    """
    events = [UserJoinedEvent(user_id=f"user_{i}", meeting_id="meeting_1") for i in range(10)]

    async def run():
        """
        Sends a sequence of events through the event mediator sequentially.
        
        Iterates over the module-scoped `events` iterable and awaits `event_mediator.send(evt)` for each event.
        """
        for evt in events:
            await event_mediator.send(evt)

    benchmark(lambda: run())


@pytest.mark.benchmark
def test_benchmark_notification_event(benchmark):
    """Benchmark notification event creation and serialization."""

    def run():
        """
        Create a `UserJoined` notification event with a `UserJoinedEvent` payload and return its serialized dictionary form.
        
        Returns:
            dict: Serialized representation of the notification event.
        """
        event = cqrs.NotificationEvent[UserJoinedEvent](
            event_name="UserJoined",
            topic="test_topic",
            payload=UserJoinedEvent(user_id="user_1", meeting_id="meeting_1"),
        )
        return event.to_dict()

    benchmark(run)
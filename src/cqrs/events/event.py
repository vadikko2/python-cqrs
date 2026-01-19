import abc
import dataclasses
import datetime
import os
import typing
import uuid
import sys

import dotenv
import pydantic

if sys.version_info >= (3, 11):
    from typing import Self  # novm
else:
    from typing_extensions import Self

dotenv.load_dotenv()
DEFAULT_OUTPUT_TOPIC = os.getenv("DEFAULT_OUTPUT_TOPIC", "output_topic")

# Type variable for generic payload types
PayloadT = typing.TypeVar("PayloadT", bound=typing.Any)


class IEvent(abc.ABC):
    """
    Interface for event-type objects.

    This abstract base class defines the contract that all event implementations
    must follow. Events represent domain events or notification events in the
    CQRS pattern and are used for communication between different parts of the system.

    All event implementations must provide:
    - `to_dict()`: Convert the event instance to a dictionary representation
    - `from_dict()`: Create an event instance from a dictionary
    """

    @abc.abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the event instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the event instance.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create an event instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the event fields.

        Returns:
            A new instance of the event class.
        """
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class DCEvent(IEvent):
    """
    Dataclass-based implementation of the event interface.

    This class provides an event implementation using Python's frozen dataclasses.
    Events are immutable (frozen=True) to ensure they cannot be modified after creation.
    It's useful when you want to avoid pydantic dependency or prefer dataclasses
    for event definitions.

    Example::

        @dataclasses.dataclass(frozen=True)
        class UserCreatedEvent(DCEvent):
            user_id: str
            username: str

        event = UserCreatedEvent(user_id="123", username="john")
        data = event.to_dict()  # {"user_id": "123", "username": "john"}
        restored = UserCreatedEvent.from_dict(**data)
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create an event instance from keyword arguments.

        Args:
            **kwargs: Keyword arguments matching the dataclass fields.

        Returns:
            A new instance of the event class.
        """
        return cls(**kwargs)

    def to_dict(self) -> dict:
        """
        Convert the event instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the dataclass instance.
        """
        return dataclasses.asdict(self)


class PydanticEvent(pydantic.BaseModel, IEvent, frozen=True):
    """
    Pydantic-based implementation of the event interface.

    This class provides an event implementation using Pydantic models with
    frozen=True to ensure immutability. It offers data validation, serialization,
    and other Pydantic features. This is the default event implementation used
    by the library.

    Events are immutable to ensure they cannot be modified after creation,
    which is important for event sourcing and event-driven architectures.

    Example::

        class UserCreatedEvent(PydanticEvent):
            user_id: str
            username: str

        event = UserCreatedEvent(user_id="123", username="john")
        data = event.to_dict()  # {"user_id": "123", "username": "john"}
        restored = UserCreatedEvent.from_dict(**data)
    """

    @classmethod
    def from_dict(cls, **kwargs) -> Self:
        """
        Create an event instance from keyword arguments.

        Validates and converts types (UUID strings to UUID objects,
        datetime strings to datetime objects, nested objects like payload).

        Args:
            **kwargs: Keyword arguments matching the event fields.

        Returns:
            A new instance of the event class.
        """
        return cls.model_validate(kwargs)

    def to_dict(self) -> dict:
        """
        Convert the event instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the event instance.
        """
        return self.model_dump(mode="python")


Event = PydanticEvent


class IDomainEvent(IEvent):
    """
    Interface for domain event objects.

    Domain events represent something that happened in the domain that domain experts
    care about. They are typically used for in-process event handling within the
    same bounded context.

    This interface extends IEvent and is implemented by DCDomainEvent and
    PydanticDomainEvent.
    """


@dataclasses.dataclass(frozen=True)
class DCDomainEvent(DCEvent, IDomainEvent):
    """
    Dataclass-based implementation of domain events.

    Domain events represent something that happened in the domain that domain experts
    care about. They are typically used for in-process event handling within the
    same bounded context.

    This is the dataclass implementation. For Pydantic-based implementation,
    use PydanticDomainEvent.

    Example::

        @dataclasses.dataclass(frozen=True)
        class OrderCreatedEvent(DCDomainEvent):
            order_id: str
            customer_id: str
            total_amount: float
    """


class PydanticDomainEvent(PydanticEvent, IDomainEvent, frozen=True):
    """
    Pydantic-based implementation of domain events.

    Domain events represent something that happened in the domain that domain experts
    care about. They are typically used for in-process event handling within the
    same bounded context.

    This is the default domain event implementation used by the library.

    Example::

        class OrderCreatedEvent(PydanticDomainEvent):
            order_id: str
            customer_id: str
            total_amount: float
    """


DomainEvent = PydanticDomainEvent


class INotificationEvent(IEvent, typing.Generic[PayloadT]):
    """
    Interface for notification event objects.

    Notification events are used for cross-service communication and are typically
    published to message brokers (Kafka, RabbitMQ, etc.). They include metadata
    like event_id, event_timestamp, event_name, and topic for routing.

    This interface extends IEvent and is implemented by DCNotificationEvent and
    PydanticNotificationEvent. It requires specific attributes that notification
    events must have.

    All notification event implementations must provide the following attributes:
    - `event_id`: uuid.UUID - Unique identifier for the event
    - `event_timestamp`: datetime.datetime - Timestamp when the event occurred
    - `event_name`: str - Name of the event type
    - `topic`: str - Message broker topic where the event should be published
    - `payload`: PayloadT - Generic payload data of type PayloadT
    """

    # These attributes must be implemented by subclasses:
    # - event_id: uuid.UUID - Unique identifier for the event
    # - event_timestamp: datetime.datetime - Timestamp when the event occurred
    # - event_name: str - Name of the event type
    # - topic: str - Message broker topic where the event should be published
    # - payload: PayloadT - Generic payload data of type PayloadT
    #
    # Type stubs for type checkers:
    if typing.TYPE_CHECKING:
        event_id: uuid.UUID
        event_timestamp: datetime.datetime
        event_name: str
        topic: str
        payload: PayloadT

        def proto(self) -> typing.Any: ...  # Method for protobuf representation


@dataclasses.dataclass(frozen=True)
class DCNotificationEvent(
    DCEvent,
    INotificationEvent[PayloadT],
    typing.Generic[PayloadT],
):
    """
    Dataclass-based implementation of notification events.

    Notification events are used for cross-service communication and are typically
    published to message brokers (Kafka, RabbitMQ, etc.). They include metadata
    like event_id, event_timestamp, event_name, and topic for routing.

    This is the dataclass implementation. For Pydantic-based implementation,
    use PydanticNotificationEvent.

    Args:
        event_id: Unique identifier for the event (auto-generated if not provided)
        event_timestamp: Timestamp when the event occurred (auto-generated if not provided)
        event_name: Name of the event type
        topic: Message broker topic where the event should be published
        payload: Generic payload data of type PayloadT

    Example::

        @dataclasses.dataclass(frozen=True)
        class UserRegisteredEvent(DCNotificationEvent[dict]):
            event_name: str = "user.registered"
            payload: dict = dataclasses.field(default_factory=lambda: {"user_id": "123"})
    """

    event_name: str
    event_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = dataclasses.field(
        default_factory=datetime.datetime.now,
    )
    topic: str = dataclasses.field(default=DEFAULT_OUTPUT_TOPIC)
    payload: PayloadT = None  # type: ignore[assignment]

    def proto(self) -> typing.Any:
        """
        Return protobuf representation of the event.

        Raises:
            NotImplementedError: This method must be implemented by subclasses
                that need protobuf serialization.
        """
        raise NotImplementedError("Method not implemented for dataclass events")

    def __hash__(self) -> int:
        """
        Return the hash of the event based on its event_id.

        Returns:
            Hash value of the event_id.
        """
        return hash(self.event_id)


class PydanticNotificationEvent(
    PydanticEvent,
    INotificationEvent[PayloadT],
    typing.Generic[PayloadT],
    frozen=True,
):
    """
    Pydantic-based implementation of notification events.

    Notification events are used for cross-service communication and are typically
    published to message brokers (Kafka, RabbitMQ, etc.). They include metadata
    like event_id, event_timestamp, event_name, and topic for routing.

    This is the default notification event implementation used by the library.

    Example::

        class UserRegisteredEvent(PydanticNotificationEvent[dict]):
            event_name: str = "user.registered"
            payload: dict = pydantic.Field(default_factory=lambda: {"user_id": "123"})
    """

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.now,
    )
    event_name: typing.Text
    topic: typing.Text = pydantic.Field(default=DEFAULT_OUTPUT_TOPIC)

    payload: PayloadT = pydantic.Field(default=None)

    model_config = pydantic.ConfigDict(from_attributes=True)

    def proto(self):
        raise NotImplementedError("Method not implemented")

    def __hash__(self):
        return hash(self.event_id)


NotificationEvent = PydanticNotificationEvent

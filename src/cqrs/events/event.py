import datetime
import os
import typing
import uuid

import dotenv
import pydantic

dotenv.load_dotenv()
DEFAULT_OUTPUT_TOPIC = os.getenv("DEFAULT_OUTPUT_TOPIC", "output_topic")

# Type variable for generic payload types
PayloadT = typing.TypeVar("PayloadT", bound=typing.Any)


class Event(pydantic.BaseModel, frozen=True):
    """
    The base class for events
    """


class DomainEvent(Event, frozen=True):
    """
    The base class for domain events
    """


class NotificationEvent(Event, typing.Generic[PayloadT], frozen=True):
    """
    The base class for notification events
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


__all__ = ("Event", "DomainEvent", "NotificationEvent")

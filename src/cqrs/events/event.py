import datetime
import os
import typing
import uuid

import dotenv
import pydantic

dotenv.load_dotenv()

DEFAULT_OUTPUT_TOPIC = os.getenv("DEFAULT_OUTPUT_TOPIC", "output_topic")


class Event(pydantic.BaseModel, frozen=True):
    """The base class for events"""


class DomainEvent(Event, frozen=True):
    """
    The base class for domain events.
    """


_P = typing.TypeVar("_P")


class NotificationEvent(Event, typing.Generic[_P], frozen=True):
    """
    The base class for notification events.

    Contains only identification information about state change.

    Example plain structure::

      {
          "event_id": "82a0b10e-1b3d-4c3c-9bdd-3934f8f824c2",
          "event_timestamp": "2023-03-06 12:11:35.103792",
          "event_name": "event_name",
          "event_type": "notification_event",
          "topic": "user_notification_events",
          "payload": {
              "changed_user_id": 987
          }
      }
    """

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.now,
    )
    event_name: typing.Text
    event_type: typing.ClassVar[typing.Text] = "notification_event"

    topic: typing.Text = pydantic.Field(default=DEFAULT_OUTPUT_TOPIC)

    payload: _P | None = pydantic.Field(default=None)

    model_config = pydantic.ConfigDict(from_attributes=True)

    def __hash__(self):
        return hash(self.event_id)


class ECSTEvent(Event, typing.Generic[_P], frozen=True):
    """
    Base class for ECST events.

    ECST means event-carried state transfer.

    Contains full information about state change.

    Example plain structure::

      {
          "event_id": "82a0b10e-1b3d-4c3c-9bdd-3934f8f824c2",
          "event_timestamp": "2023-03-06 12:11:35.103792",
          "event_name": "event_name",
          "event_type": "ecst_event",
          "topic": "user_ecst_events",
          "payload": {
              "user_id": 987,
              "new_user_last_name": "Doe",
              "new_user_nickname": "kend"
          }
      }

    """

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.now,
    )
    event_name: typing.Text
    event_type: typing.ClassVar = "ecst_event"

    topic: typing.Text = pydantic.Field(default=DEFAULT_OUTPUT_TOPIC)

    payload: _P | None = pydantic.Field(default=None)

    model_config = pydantic.ConfigDict(from_attributes=True)

    def __hash__(self):
        return hash(self.event_id)

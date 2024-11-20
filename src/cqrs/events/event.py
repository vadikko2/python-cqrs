import datetime
import logging
import os
import typing
import uuid

import dotenv
import pydantic

logger = logging.getLogger("cqrs")

try:
    from google.protobuf.message import Message  # noqa
except ImportError:
    logger.warning(
        "Please install protobuf dependencies with: `pip install python-cqrs[protobuf]`",
    )

dotenv.load_dotenv()

DEFAULT_OUTPUT_TOPIC = os.getenv("DEFAULT_OUTPUT_TOPIC", "output_topic")


class Event(pydantic.BaseModel, frozen=True):
    """
    The base class for events
    """


class DomainEvent(Event, frozen=True):
    """
    The base class for domain events
    """


_P = typing.TypeVar("_P", typing.Any, None)


class NotificationEvent(Event, typing.Generic[_P], frozen=True):
    """
    The base class for notification events
    """

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    event_timestamp: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.now,
    )
    event_name: typing.Text
    topic: typing.Text = pydantic.Field(default=DEFAULT_OUTPUT_TOPIC)

    payload: _P = pydantic.Field(default=None)

    model_config = pydantic.ConfigDict(from_attributes=True)

    def proto(self) -> "Message":
        raise NotImplementedError("Method not implemented")

    def __hash__(self):
        return hash(self.event_id)

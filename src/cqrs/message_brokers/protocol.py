import abc
import typing
import uuid

import pydantic


class Message(pydantic.BaseModel):
    message_type: typing.Text = pydantic.Field()
    message_name: typing.Text = pydantic.Field()
    message_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    topic: typing.Text
    payload: typing.Dict


class MessageBroker(abc.ABC):
    """
    The interface over a message broker.

    Used for sending messages to message brokers (currently only redis supported).
    """

    @abc.abstractmethod
    async def send_message(self, message: Message) -> None:
        raise NotImplementedError

import typing
import uuid

import pydantic


class Message(pydantic.BaseModel):
    message_type: str = pydantic.Field()
    message_name: str = pydantic.Field()
    message_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    payload: dict = pydantic.Field()


class MessageBroker(typing.Protocol):
    """
    The interface over a message broker.

    Used for sending messages to message brokers (currently only redis supported).
    """

    async def send_message(self, message: Message) -> None:
        ...

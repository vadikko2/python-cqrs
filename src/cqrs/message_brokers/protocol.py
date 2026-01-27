import abc
import dataclasses
import typing
import uuid
from dataclass_wizard import asdict


@dataclasses.dataclass
class Message:
    """
    Internal message structure for message broker communication.

    Args:
        message_name: Name of the message type
        message_id: Unique identifier for the message (auto-generated if not provided)
        topic: Message broker topic where the message should be sent
        payload: Message payload data
    """

    message_name: typing.Text
    topic: typing.Text
    payload: typing.Any
    message_id: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)

    def to_dict(self) -> dict[str, typing.Any]:
        """
        Convert the message instance to a dictionary representation.

        Returns:
            A dictionary containing all fields of the message instance.
        """
        return asdict(self)


class MessageBroker(abc.ABC):
    """
    The interface over a message broker.

    Used for sending messages to message brokers (currently only redis supported).
    """

    @abc.abstractmethod
    async def send_message(self, message: Message) -> None:
        raise NotImplementedError

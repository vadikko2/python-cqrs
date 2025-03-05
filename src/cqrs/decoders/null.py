import typing

from faststream import kafka, types


async def empty_message_decoder(
    msg: kafka.KafkaMessage,
    original_decoder: typing.Callable[[kafka.KafkaMessage], typing.Awaitable[types.DecodedMessage]],
) -> types.DecodedMessage | None:
    """
    Decode a kafka message and return it if it is not empty.
    """
    if not msg.body:
        return None
    return await original_decoder(msg)

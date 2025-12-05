import typing

try:
    from faststream import kafka, types
except ImportError:
    print(
        'Please install faststream with kafka supporting (pip install "faststream[kafka]") for use kafka message decoder')
    raise


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

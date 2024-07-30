import logging

from cqrs.message_brokers import protocol

logger = logging.getLogger("cqrs")


class DevnullMessageBroker:
    async def send_message(self, message: protocol.Message) -> None:
        logger.warning(f"Event {message} will be skip")

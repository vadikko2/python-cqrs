import logging

from cqrs.message_brokers import protocol

logger = logging.getLogger("cqrs")

MESSAGE_BUS = []


class DevnullMessageBroker(protocol.MessageBroker):
    async def send_message(self, message: protocol.Message) -> None:
        MESSAGE_BUS.append(message)
        logger.warning(f"Event {message} will be skip")

import logging
import typing

from cqrs.adapters import kafka
from cqrs.message_brokers import protocol


class KafkaMessageBroker(protocol.MessageBroker):
    def __init__(
        self,
        producer: kafka.KafkaProducer,
        aiokafka_log_level: typing.Text = "ERROR",
    ):
        self._producer = producer
        logging.getLogger("aiokafka").setLevel(aiokafka_log_level)

    async def send_message(self, message: protocol.Message) -> None:
        await self._producer.produce(message.topic, message.payload)

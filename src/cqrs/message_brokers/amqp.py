import logging

import aio_pika
import orjson

from cqrs.adapters import amqp
from cqrs.message_brokers import protocol


class AMQPMessageBroker(protocol.MessageBroker):
    def __init__(self, dsn: str, exchange_name: str, pika_log_level: str = "ERROR"):
        self.publisher = amqp.AMQPPublisher(url=dsn)
        self.exchange_name = exchange_name
        logging.getLogger("aiormq").setLevel(pika_log_level)
        logging.getLogger("aio_pika").setLevel(pika_log_level)

    async def send_message(self, message: protocol.Message) -> None:
        await self.publisher.publish(
            message=aio_pika.Message(body=orjson.dumps(message.payload)),
            exchange_name=self.exchange_name,
            queue_name=message.topic,
        )

import asyncio
import functools
import logging
import typing

import aiokafka
import orjson
import retry_async
from aiokafka import errors

__all__ = (
    "KafkaProducer",
    "kafka_producer_factory",
)

_retry = functools.partial(
    retry_async.retry,
    exceptions=(
        errors.KafkaConnectionError,
        errors.NodeNotReadyError,
        errors.RequestTimedOutError,
    ),
    is_async=True,
)

SecurityProtocol: typing.TypeAlias = typing.Literal[
    "PLAINTEXT",
    "SSL",
    "SASL_PLAINTEXT",
    "SASL_SSL",
]
SaslMechanism: typing.TypeAlias = typing.Literal[
    "PLAIN",
    "GSSAPI",
    "SCRAM-SHA-256",
    "SCRAM-SHA-512",
    "OAUTHBEARER",
]

logger = logging.getLogger("cqrs")
logger.setLevel(logging.DEBUG)


def _serializer(message: typing.Dict) -> typing.ByteString:
    return orjson.dumps(message)


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class KafkaProducer(metaclass=_Singleton):
    def __init__(
        self,
        producer: aiokafka.AIOKafkaProducer,
        retry_count: int = 3,
        retry_delay: int = 1,
    ):
        self._producer = producer
        self._retry_count = retry_count
        self._retry_delay = retry_delay

    async def _check_connection(self):
        node_id = self._producer.client.get_random_node()
        if not await self._producer.client.ready(node_id=node_id):
            await self._producer.start()

    async def _produce(self, topic: typing.Text, message: typing.Dict):
        await self._check_connection()
        logger.debug(f"produce message {message} to topic {topic}")
        await self._producer.send_and_wait(topic, value=message)

    async def produce(self, topic: typing.Text, message: typing.Dict):
        """
        Produces event to kafka broker.
        Tries to reconnect if connect has been lost or has not been opened.
        """
        await _retry(tries=self._retry_count, delay=self._retry_delay)(self._produce)(
            topic,
            message,
        )


def kafka_producer_factory(
    dsn: typing.Text,
    security_protocol: SecurityProtocol = "PLAINTEXT",
    sasl_mechanism: SaslMechanism = "PLAIN",
    retry_count: int = 3,
    retry_delay: int = 1,
    user: typing.Text | None = None,
    password: typing.Text | None = None,
) -> KafkaProducer:
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)

    producer = aiokafka.AIOKafkaProducer(
        bootstrap_servers=dsn,
        value_serializer=_serializer,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=user,
        sasl_plain_password=password,
        loop=loop,
    )
    return KafkaProducer(
        producer=producer,
        retry_count=retry_count,
        retry_delay=retry_delay,
    )

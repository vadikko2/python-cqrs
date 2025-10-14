import asyncio
import typing
from functools import partial

from cqrs.adapters import protocol

import aio_pika
from aio_pika import abc, pool


async def connection_pool_factory(url: str) -> abc.AbstractRobustConnection:
    return await aio_pika.connect_robust(url=url)


async def channel_pool_factory(
    connection_pool: pool.Pool[aio_pika.abc.AbstractConnection],
) -> aio_pika.abc.AbstractChannel:
    async with connection_pool.acquire() as connection:
        return await connection.channel()


class AMQPPublisher(protocol.AMQPPublisher):
    def __init__(self, channel_pool: pool.Pool[aio_pika.abc.AbstractChannel]):
        self.channel_pool = channel_pool

    async def publish(self, message: abc.AbstractMessage, queue_name: str, exchange_name: str) -> None:
        async with self.channel_pool.acquire() as channel:
            queue = await channel.declare_queue(queue_name)
            exchange = await channel.declare_exchange(exchange_name, type="direct", auto_delete=True)
            await queue.bind(exchange=exchange, routing_key=queue_name)
            await exchange.publish(message=message, routing_key=queue_name)


class AMQPConsumer(protocol.AMQPConsumer):
    def __init__(self, channel_pool: pool.Pool[aio_pika.abc.AbstractChannel]):
        self.channel_pool = channel_pool

    async def consume(
        self,
        handler: typing.Callable[[abc.AbstractIncomingMessage], typing.Awaitable[None]],
        queue_name: str,
    ) -> None:
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=1)
            queue = await channel.declare_queue(queue_name)
            await queue.consume(handler)
            await asyncio.Future()


def amqp_publisher_factory(
    url: typing.Text,
    max_connection_pool_size: int = 2,
    max_channel_pool_size: int = 10,
) -> AMQPPublisher:
    max_connection_pool_size = max_connection_pool_size
    max_channel_pool_size = max_channel_pool_size
    connection_pool = pool.Pool(
        partial(connection_pool_factory, url=url),
        max_size=max_connection_pool_size,
    )
    channel_pool = pool.Pool(
        partial(channel_pool_factory, connection_pool=connection_pool),
        max_size=max_channel_pool_size,
    )
    return AMQPPublisher(channel_pool=channel_pool)


def amqp_consumer_factory(
    url: typing.Text,
    max_connection_pool_size: int = 2,
    max_channel_pool_size: int = 10,
) -> AMQPConsumer:
    max_connection_pool_size = max_connection_pool_size
    max_channel_pool_size = max_channel_pool_size
    connection_pool = pool.Pool(
        partial(connection_pool_factory, url=url),
        max_size=max_connection_pool_size,
    )
    channel_pool = pool.Pool(
        partial(channel_pool_factory, connection_pool=connection_pool),
        max_size=max_channel_pool_size,
    )
    return AMQPConsumer(channel_pool=channel_pool)

import asyncio
import typing
from functools import partial

import aio_pika
from aio_pika import abc, pool


async def connection_pool_factory(url: str) -> abc.AbstractRobustConnection:
    return await aio_pika.connect_robust(url=url)


async def channel_pool_factory(connection_pool: pool.Pool) -> aio_pika.Channel:
    async with connection_pool.acquire() as connection:
        return await connection.channel()


class AMQPPublisher:
    def __init__(self, url: str, max_connection_pool_size=2, max_channel_pool_size=10):
        self.url = url
        self.max_connection_pool_size = max_connection_pool_size
        self.max_channel_pool_size = max_channel_pool_size
        self.connection_pool: pool.Pool = pool.Pool(
            partial(connection_pool_factory, url=url),
            max_size=self.max_connection_pool_size,
        )
        self.channel_pool: pool.Pool = pool.Pool(
            partial(channel_pool_factory, connection_pool=self.connection_pool),
            max_size=self.max_channel_pool_size,
        )

    async def publish(self, message: abc.AbstractMessage, queue_name: str, exchange_name: str) -> None:
        async with self.channel_pool.acquire() as channel:
            queue: aio_pika.Queue = await channel.declare_queue(queue_name)
            exchange: aio_pika.Exchange = await channel.declare_exchange(exchange_name, type="direct", auto_delete=True)
            await queue.bind(exchange=exchange, routing_key=queue_name)
            await exchange.publish(message=message, routing_key=queue_name)


class AMQPConsumer:
    def __init__(self, url: str, max_connection_pool_size=2, max_channel_pool_size=10):
        self.url = url
        self.max_connection_pool_size = max_connection_pool_size
        self.max_channel_pool_size = max_channel_pool_size
        self.connection_pool: pool.Pool = pool.Pool(
            partial(connection_pool_factory, url=url),
            max_size=self.max_connection_pool_size,
        )
        self.channel_pool: pool.Pool = pool.Pool(
            partial(channel_pool_factory, connection_pool=self.connection_pool),
            max_size=self.max_channel_pool_size,
        )

    async def consume(
        self,
        handler: typing.Callable[[abc.AbstractIncomingMessage], typing.Awaitable[None]],
        queue_name: str,
    ) -> None:
        async with self.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=1)
            queue = await channel.declare_queue(queue_name)
            await queue._consume(handler)
            await asyncio.Future()

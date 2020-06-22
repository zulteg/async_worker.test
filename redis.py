import asyncio
import json
import logging
from asyncio import AbstractEventLoop

from aioredis import create_pool, ConnectionsPool

from utils import AsyncTaskLogger

logger = logging.getLogger(__name__)


class AsyncRedisClient(AsyncTaskLogger):
    loop: AbstractEventLoop = None
    pool: ConnectionsPool = None

    def __init__(self, host, port, prefix, name, minsize=5, maxsize=10):
        self.minsize = minsize
        self.maxsize = maxsize

        self.host = host
        self.port = port
        self.prefix = prefix
        self.name = name

        self.queue_tasks = f'{self.prefix}:{self.name}'
        self.queue_processing = f'{self.prefix}:{self.name}:processing'
        self.queue_rejected = f'{self.prefix}:{self.name}:rejected'

        self.sleep_after_io_error = 5

    def set_loop(self, loop: AbstractEventLoop):
        self.loop = loop

    async def connect(self):
        while True:
            try:
                self.pool = await create_pool(
                    f"redis://{self.host}:{self.port}",
                    minsize=self.minsize,
                    maxsize=self.maxsize,
                    loop=self.loop
                )
                self.log_info('Redis connect success')
                break
            except IOError as e:
                self.log_warning('Redis connection error, retry in %ss %s' % (self.sleep_after_io_error, e))
                await asyncio.sleep(self.sleep_after_io_error, loop=self.loop)

    async def get_queue_task(self):
        while True:
            try:
                return await self.pool.execute('RPOPLPUSH', self.queue_tasks, self.queue_processing)
            except IOError:
                await self.connect()

    async def retry_processing_task(self):
        while True:
            try:
                return await self.pool.execute('RPOPLPUSH', self.queue_processing, self.queue_tasks)
            except IOError:
                await self.connect()

    async def add_queue_task(self, task):
        while True:
            try:
                return await self.pool.execute('LPUSH', self.queue_tasks, task)
            except IOError:
                await self.connect()

    async def add_reject_task(self, task):
        while True:
            try:
                return await self.pool.execute('LPUSH', self.queue_rejected, task)
            except IOError:
                await self.connect()

    async def rm_processing_task(self, task):
        while True:
            try:
                return await self.pool.execute('LREM', self.queue_processing, 0, task)
            except IOError:
                await self.connect()

    async def queue_len(self):
        while True:
            try:
                return await self.pool.execute('LLEN', self.queue_tasks)
            except IOError:
                await self.connect()

    async def processing_len(self):
        while True:
            try:
                return await self.pool.execute('LLEN', self.queue_processing)
            except IOError:
                await self.connect()

    async def get_key(self, key):
        while True:
            try:
                data = await self.pool.execute('GET', key)
                if data:
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        data = data.decode('utf-8')
                return data
            except IOError:
                await self.connect()

    async def set_key(self, key, data, expire=None):
        while True:
            try:
                try:
                    data = json.dumps(data)
                except TypeError:
                    data = data
                _return = await self.pool.execute('SET', key, data)
                if expire:
                    await self.pool.execute('EXPIRE', key, expire)
                return _return
            except IOError:
                await self.connect()

    async def del_key(self, key):
        while True:
            try:
                return await self.pool.execute('DEL', key)
            except IOError:
                await self.connect()

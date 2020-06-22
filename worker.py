import asyncio
import logging
from asyncio import AbstractEventLoop

from colorama import Fore

from exceptions import AsyncTaskClassError, AsyncTaskNameError
from redis import AsyncRedisClient
from task import AsyncTaskFactory, AsyncTask, AsyncSchedule, AsyncScheduleTask
from utils import AsyncRequestError

logger = logging.getLogger(__name__)


class AsyncWorker:
    loop: AbstractEventLoop = None
    redis: AsyncRedisClient = None

    def __init__(self, factory: AsyncTaskFactory, schedule: AsyncSchedule, redis_host: str, redis_port: int,
                 prefix: str):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.sleep_after_loop = 5
        self.sleep_after_io_error = 5

        self.factory = factory
        self.schedule = schedule
        self.schedule_sleep_by = 12 * 60 * 60

        self.redis = AsyncRedisClient(
            host=redis_host,
            port=redis_port,
            prefix=prefix,
            name='worker'
        )

        self.known_exceptions = (AsyncRequestError,)

    def run(self):
        self.loop.create_task(self.init())
        self.loop.run_forever()

    @staticmethod
    def log_warning(s, exc_info=True):
        logger.warning(Fore.LIGHTRED_EX + f'{s}' + Fore.LIGHTWHITE_EX, exc_info=exc_info)

    @staticmethod
    def log_info(s):
        logger.info(Fore.LIGHTBLUE_EX + f'{s}' + Fore.LIGHTWHITE_EX)

    async def redis_connect(self):
        """Create connection to Redis"""
        self.redis.set_loop(loop=self.loop)
        await self.redis.connect()

    async def init_schedule(self):
        """Add schedule tasks to loop"""
        for scheduleTask in self.schedule.tasks:
            self.log_info('Init schedule task: %s' % (scheduleTask,))
            self.loop.create_task(self.schedule_task_run(scheduleTask))

    async def schedule_task_run(self, schedule_task: AsyncScheduleTask):
        """Add task to queue by schedule period"""
        while True:
            wait = abs(schedule_task.wait())
            while wait > 0:
                sleep_time = self.schedule_sleep_by if wait > self.schedule_sleep_by else wait
                wait -= sleep_time
                await asyncio.sleep(sleep_time, loop=self.loop)

            await self.redis.add_queue_task(schedule_task.task.serialize())

    async def retry_processing_tasks(self):
        """Add processing tasks to queue"""
        for _ in range(await self.redis.processing_len()):
            task_str = await self.redis.retry_processing_task()
            self.log_info('Add processing task: %s' % (task_str,))

    async def init(self):
        """Init worker"""
        # get redis connection
        await self.redis_connect()

        # init schedule tasks
        await self.init_schedule()

        # move processing tasks to tasks queue
        await self.retry_processing_tasks()

        # run tasks loop
        self.loop.create_task(self.tasks_loop())
        self.log_info('AsyncWorker started')

    async def tasks_loop(self):
        """Getting tasks from Redis and adding to loop"""
        while True:
            for _ in range(await self.redis.queue_len()):
                task_str = await self.redis.get_queue_task()

                if not task_str:
                    continue

                try:
                    task = self.factory.create(task_str)
                except (AsyncTaskClassError, AsyncTaskNameError) as e:
                    self.log_warning(e, exc_info=False)
                    await self.redis.rm_processing_task(task_str)
                    continue
                except Exception as e:
                    self.log_warning('Unhandled exception %s' % (e,))
                    await self.redis.rm_processing_task(task_str)
                    continue

                task.set_loop(self.loop)
                task.set_redis(self.redis)

                self.loop.create_task(self.task_run(task))

            await asyncio.sleep(self.sleep_after_loop, loop=self.loop)

    async def task_run(self, task: AsyncTask):
        """Run task"""
        if task.log_run:
            self.log_info('Run      %s' % (task,))

        try:
            await task.run()
        except self.known_exceptions as e:
            error = '%s known exception %s' % (task.__class__.__name__, e,)
            self.log_warning(error, exc_info=False)
            task.set_error(error)
            await self.task_retry(task)
            return
        except Exception as e:
            error = '%s unknown exception %s' % (task.__class__.__name__, e,)
            self.log_warning(error)
            task.set_error(error)
            await self.task_retry(task)
            return

        if task.log_complete:
            self.log_info('Complete %s' % (task,))

        await self.redis.rm_processing_task(task.serialize())

    async def task_retry(self, task: AsyncTask):
        """Check task retries and add task to queue by retry interval or reject it"""
        old_task_str = task.serialize()

        if task.add_retry():
            await asyncio.sleep(task.retries_interval, loop=self.loop)
            await self.redis.rm_processing_task(old_task_str)
            await self.redis.add_queue_task(task.serialize())
        else:
            await self.redis.rm_processing_task(old_task_str)
            self.log_rejected_task(task=task)
            self.log_warning('Reject %s' % (task,), exc_info=False)

    @staticmethod
    def log_rejected_task(task: AsyncTask):
        LogsJournal.objects.using('journal').create(
            type=LogsJournal.TYPE_WARNING,
            level=LogsJournal.LEVEL_WORKER,
            action_type=LogsJournal.ACTION_TYPE_NONE,
            msg=f'Reject task {task.__class__.__name__}',
            data=dict(
                task=task.to_dict(),
                error=task.get_error()
            )
        )

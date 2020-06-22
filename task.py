import json
import logging
import math
import time
from abc import abstractmethod, ABC
from asyncio import AbstractEventLoop
from typing import Type, Iterator

from croniter import croniter
from django.utils import timezone

from exceptions import AsyncTaskClassError, AsyncTaskNameError
from redis import AsyncRedisClient
from utils import AsyncTaskLogger

logger = logging.getLogger(__name__)


class AsyncTask(ABC, AsyncTaskLogger):
    data: dict = None
    data_fields: tuple = None
    retries: int = 0
    max_retries: int = 0
    retries_interval: int = 0

    loop: AbstractEventLoop = None
    redis: AsyncRedisClient = None
    error: str = ''
    log_run: bool = True
    log_complete: bool = True

    def __init__(self, data: dict = None, retries: int = None, max_retries: int = None, retries_interval: int = None):
        self.data = data if data else self.__class__.data if self.__class__.data else {}
        self.retries = retries if retries else self.__class__.retries
        self.max_retries = max_retries if max_retries else self.__class__.max_retries
        self.retries_interval = retries_interval if retries_interval else self.__class__.retries_interval

    def set_loop(self, loop: AbstractEventLoop):
        self.loop = loop

    def set_redis(self, redis: AsyncRedisClient):
        self.redis = redis

    def to_dict(self) -> dict:
        return {
            '__class__': self.__class__.__name__,
            'data': self.data,
            'retries': self.retries,
            'max_retries': self.max_retries,
            'retries_interval': self.retries_interval
        }

    def serialize(self) -> str:
        return json.dumps(self.to_dict())

    def add_retry(self) -> bool:
        if self.retries != self.max_retries:
            self.retries += 1
            return True
        return False

    def __str__(self):
        return f"{self.__class__.__name__} {self.retries}/{self.max_retries} ({self.retries_interval}s)"

    def set_error(self, error: str) -> None:
        self.error = str(error)

    def get_error(self) -> str:
        return self.error

    def messages(self) -> list:
        return [self.serialize()]

    async def run(self) -> None:
        self.error = ''
        await self._run()

    @abstractmethod
    async def _run(self) -> None:
        pass


class AsyncTaskFactory:
    def __init__(self):
        self._builders = {}

    def register_builder(self, builder: Type[AsyncTask]) -> None:
        self._builders[builder.__name__] = builder

    def get_builder(self, key: str) -> Type[AsyncTask]:
        return self._builders.get(key, None)

    def create(self, task_str: str) -> AsyncTask:
        obj_dict = json.loads(task_str)

        class_name = obj_dict.pop('__class__', None)
        if not class_name:
            raise AsyncTaskClassError(f"There is no '__class__' key in obj_dict")

        builder = self.get_builder(class_name)
        if not builder:
            raise AsyncTaskNameError(f"There is no builder for class: {class_name}")

        return builder(**obj_dict)

    @property
    def builders(self):
        return self._builders


class AsyncScheduleTask(ABC):
    def __init__(self, task: AsyncTask, pattern: str, seconds: int):
        self.task = task

    @abstractmethod
    def next(self) -> int:
        pass

    def wait(self) -> int:
        return int(self.next() - time.time())


class AsyncScheduleTaskCron(AsyncScheduleTask):
    def __init__(self, task: AsyncTask, pattern: str = '', seconds: int = 0):
        super().__init__(task, pattern, seconds)
        self._pattern = pattern
        start_time = timezone.localtime()
        self._croniter = croniter(self._pattern, start_time=start_time)

    def __str__(self):
        return f"{self.task}: cron '{self._pattern}'"

    def next(self) -> int:
        return self._croniter.get_next()


class AsyncScheduleTaskTime(AsyncScheduleTask):
    def __init__(self, task: AsyncTask, pattern: str = '', seconds: int = 0):
        super().__init__(task, pattern, seconds)
        self._seconds = seconds
        self._current = time.time()

    def __str__(self):
        return f"{self.task}: every {self._seconds}s"

    def next(self) -> int:
        self._current = int((math.floor(self._current / self._seconds) + 1) * self._seconds)
        return self._current


class AsyncSchedule:
    def __init__(self):
        self._tasks = []

    def add_task(self, task: AsyncTask, pattern: str = '', seconds: int = 0) -> None:
        if pattern:
            self._tasks.append(AsyncScheduleTaskCron(task=task, pattern=pattern))
        elif seconds:
            self._tasks.append(AsyncScheduleTaskTime(task=task, seconds=seconds))

    @property
    def tasks(self) -> Iterator[AsyncScheduleTask]:
        return iter(self._tasks)

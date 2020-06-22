import threading
import time

from django.apps import AppConfig
from django.conf import settings


def process():
    from worker import AsyncWorker
    from config import config_factory, config_schedule
    from core.utils import prefix_hash

    time.sleep(5)

    async_worker = AsyncWorker(
        factory=config_factory(),
        schedule=config_schedule(),
        redis_host=settings.REDIS_HOST,
        redis_port=settings.REDIS_PORT,
        prefix=prefix_hash()
    )
    async_worker.run()


class WorkerAppConfig(AppConfig):
    name = 'worker'

    def ready(self):
        if not settings.MAIN:
            return

        t = threading.Thread(target=process)
        t.setDaemon(True)
        t.start()

import logging

from task import AsyncTask

logger = logging.getLogger(__name__)


class AsyncTaskEveryPeriod(AsyncTask):
    """ Test task """

    async def _run(self) -> None:
        self.log_info('I am a test task')

from task import AsyncTaskFactory, AsyncSchedule
from tasks.common import AsyncTaskEveryPeriod


def config_factory() -> AsyncTaskFactory:
    factory = AsyncTaskFactory()

    factory.register_builder(AsyncTaskEveryPeriod)

    return factory


def config_schedule() -> AsyncSchedule:
    """
    by pattern:
        schedule.add_task(AsyncTaskEveryPeriod(), pattern='0 0 */1 * *')
    every seconds:
        schedule.add_task(AsyncTaskEveryPeriod(), seconds=30)
    """
    schedule = AsyncSchedule()

    schedule.add_task(AsyncTaskEveryPeriod(), seconds=60)

    return schedule

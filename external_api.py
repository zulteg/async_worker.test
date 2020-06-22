import logging

from utils import AsyncTaskLogger, AsyncTaskHttpExternalApiRequest

logger = logging.getLogger(__name__)


class ExternalApiBase(AsyncTaskLogger):
    url: str
    token: str
    token_type: str
    device_id: str

    def __init__(self, url: str, token: str, token_type: str, device_id: str):
        self.url = url
        self.token = token
        self.token_type = token_type
        self.device_id = device_id

    async def send(self, data, msg):
        await AsyncTaskHttpExternalApiRequest(
            url=self.url,
            auth_token=self.token,
            auth_token_type=self.token_type,
            data=data,
            msg=f'{self.__class__.__name__} - {msg}',
            device_id=self.device_id
        ).send()

    async def device_status(self, task_data) -> None:
        data = dict()

        await self.send(data=data, msg='device_status')

    async def cell_status(self, task_data) -> None:
        data = dict()

        await self.send(data=data, msg='cell_status')

    async def order_status(self, task_data) -> None:
        data = dict()

        await self.send(data=data, msg='order_status')

    async def order_statuses(self, task_data) -> None:
        data = dict()

        await self.send(data=data, msg='order_statuses')


class ExternalApiAlt(ExternalApiBase):
    async def order_status(self, task_data) -> None:
        data = dict()

        await self.send(data=data, msg='order_status')


class ExternalApiService(AsyncTaskLogger):
    version: str
    service: ExternalApiBase

    def __init__(self, version: str, url: str, token: str, token_type: str, device_id: str):
        self.version = version
        self.service = self.init_service(url, token, token_type, device_id)
        if not self.service:
            self.log_warning(f'Unknown external API version: {version}')
            self.log_journal(
                type=LogsJournal.TYPE_WARNING,
                msg=f'Unknown external API version: {version}'
            )

    def init_service(self, url: str, token: str, token_type: str, device_id: str):
        if self.version == EXTERNAL_API_VERSION_BASE:
            return ExternalApiBase(url, token, token_type, device_id)
        elif self.version == EXTERNAL_API_VERSION_ALT:
            return ExternalApiAlt(url, token, token_type, device_id)

    async def device_status(self, data) -> None:
        if self.service:
            await self.service.device_status(data)

    async def cell_status(self, data) -> None:
        if self.service:
            await self.service.cell_status(data)

    async def order_status(self, data) -> None:
        if self.service:
            await self.service.order_status(data)

    async def order_statuses(self, data) -> None:
        if self.service:
            await self.service.order_statuses(data)

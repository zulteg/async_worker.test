import asyncio
import logging

import aiohttp
from colorama import Fore

logger = logging.getLogger(__name__)


class AsyncTaskLogger:
    def log_info(self, s):
        logger.info(
            Fore.LIGHTBLUE_EX + f'{self.__class__.__name__}: ' + Fore.GREEN + f'{s}' + Fore.LIGHTWHITE_EX
        )

    def log_info_alt(self, s):
        logger.info(
            Fore.LIGHTBLUE_EX + f'{self.__class__.__name__}: ' + Fore.LIGHTGREEN_EX + f'{s}' + Fore.LIGHTWHITE_EX
        )

    def log_warning(self, s):
        logger.info(
            Fore.LIGHTBLUE_EX + f'{self.__class__.__name__}: ' + Fore.LIGHTRED_EX + f'{s}' + Fore.LIGHTWHITE_EX
        )

    def log_journal(self, msg='', data=None, type=LogsJournal.TYPE_INFO):
        LogsJournal.objects.using('journal').create(
            type=type,
            level=LogsJournal.LEVEL_WORKER,
            action_type=LogsJournal.ACTION_TYPE_NONE,
            msg=f'{self.__class__.__name__} - {msg}',
            data=data
        )


class AsyncRequestError(Exception):
    """ A wrapper of all possible exception during a HTTP request """

    def __init__(self, raised='', message='', status_code=0, request=None):
        self.raised = raised
        self.message = message
        self.status_code = status_code
        self.request = request
        super().__init__(f"raised={self.raised} message={self.message} "
                         f"request={self.request} status_code={self.status_code}")


class AsyncTaskHttpClient:
    def __init__(self, session, timeout=60):
        self.session = session
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def post(self, url, data, headers):
        try:
            async with self.session.post(url, json=data, headers=headers, timeout=self.timeout, ssl=False) as response:
                try:
                    response_data = await response.json()
                except aiohttp.ContentTypeError:
                    response_data = await response.text()
                return response_data, response.status
        except (asyncio.TimeoutError,
                aiohttp.ClientConnectorError,
                aiohttp.ServerTimeoutError,
                aiohttp.ServerConnectionError,
                aiohttp.ServerDisconnectedError) as e:
            raise AsyncRequestError(
                raised=e.__class__.__name__,
                message=str(e),
                request={'url': url, 'data': data, 'headers': headers},
                status_code=500
            )
        except Exception as e:
            raise e


class AsyncTaskHttpExternalApiRequest:
    def __init__(self, url, data, auth_token, auth_token_type, msg='', device_id=None):
        self.url = url
        self.data = data
        self.auth_token = auth_token
        self.auth_token_type = auth_token_type

        self.msg = msg
        self.device_id = device_id

        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'{self.auth_token_type} {self.auth_token}',
        }
        self.basic_status = 200
        self.error_status = 500

    async def send(self):
        logs = list()
        logs.append(LogsJournal(
            ip=self.device_id,
            msg=self.msg,
            type=LogsJournal.TYPE_INFO,
            level=LogsJournal.LEVEL_EXT_API,
            action_type=LogsJournal.ACTION_TYPE_REQUEST,
            data=dict(
                url=self.url,
                headers=self.headers,
                data=self.data
            ),
            status_code=self.basic_status
        ))

        status_code = self.error_status
        raise_exception = False
        try:
            async with aiohttp.ClientSession() as session:
                response_data, status_code = await AsyncTaskHttpClient(session=session).post(
                    url=self.url, data=self.data, headers=self.headers
                )
        except AsyncRequestError as e:
            raise_exception = e
            response_data = {'error': e.__dict__}
            status_code = e.status_code
        except Exception as e:
            raise_exception = e
            response_data = {'error': str(e)}

        logs.append(LogsJournal(
            ip=self.device_id,
            msg=self.msg,
            type=LogsJournal.TYPE_INFO if status_code == 200 else LogsJournal.TYPE_WARNING,
            level=LogsJournal.LEVEL_EXT_API,
            action_type=LogsJournal.ACTION_TYPE_RESPONSE,
            data=response_data,
            status_code=status_code
        ))
        for log in logs:
            log.save(using='journal')

        if raise_exception:
            raise raise_exception

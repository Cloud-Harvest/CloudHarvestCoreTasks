from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.environment import Environment
from CloudHarvestCoreTasks.tasks.base import BaseTask

from logging import getLogger
from requests import Response
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ConnectTimeout,
    HTTPError,
    ProxyError,
    ReadTimeout,
    SSLError,
    TooManyRedirects
)

RETRYABLE_EXCEPTIONS = (
    ChunkedEncodingError,
    ConnectionError,
    ConnectTimeout,
    ProxyError,
    ReadTimeout,
    SSLError,
    TooManyRedirects
)

RETRYABLE_HTTP_STATUS_CODES = (
    408,  # Request Timeout
    409,  # Conflict
    429,  # Too Many Requests
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
    507   # Insufficient Storage
)

logger = getLogger('harvest')


@register_definition(name='enqueue', category='task')
class EnqueueTask(BaseTask):
    def __init__(self, template: dict or str, api: dict = None, variables: dict = None, *args, **kwargs):
        """
        The EnqueueTask class is used to submit, await, and retrieve the results of a task using the Cloud Harvest API.
        These kinds of subtasks are necessary when the calling task's agent may not have the necessary resources to
        execute the task directly, such as when it requires a specific platform, account, or region to execute.

        Arguments
        template: (dict or str) The template for the task to be executed. If a string is provided, it will be treated as the name of the template.
        api: (dict) Configuration for the API connection, including host, port, SSL settings, and token.
        variables: (dict) Variables to be passed to the task when it is executed.
        """

        # API Configuration
        self.api_host = api.get('host') or Environment.get('api.host')
        self.api_port = api.get('port') or Environment.get('api.port')
        self.api_ssl = api.get('ssl') or Environment.get('api.ssl')
        self.api_token = api.get('token') or Environment.get('api.token')
        self.api_attempts = api.get('attempts') or 10

        # Template configuration
        if isinstance(template, str):
            template = {'name': template, 'type': 'reports'}

        self.template_name = template.get('name')
        self.template_type = template.get('type')

        self.variables = variables or {}

        super().__init__(*args, **kwargs)

    def method(self) -> 'BaseTask':
        # First we submit the new task to tasks/queue_task
        data = {
                'variables': self.variables
            }

        if self.task_chain:
            data['parent'] = self.task_chain.id
            priority = self.task_chain.priority

        else:
            priority = 1

        response = self._api_request(
            method='post',
            endpoint=f'tasks/queue_task/{priority}/{self.template_type}/{self.template_name}',
            data={
                'variables': self.variables
            }
        )

        if not isinstance(response, Response):
            raise Exception(f'Task failed to enqueue') from response

        new_task_id = response.json().get('result').get('id')

        # Then we await the response via tasks/await_task
        response = self._api_request(
            method='get',
            endpoint=f'tasks/await_task/{new_task_id}'
        )

        if not isinstance(response, Response):
            raise Exception(f'Failed to await task {new_task_id}') from response

        if response.json().get('success') is False:
            raise Exception(f'Task {new_task_id} failed to complete: {response.json().get("reason")}')

        # And finally we return the results using tasks/get_task_results
        response = self._api_request(
            method='get',
            endpoint=f'tasks/get_task_results/{new_task_id}'
        )

        if not isinstance(response, Response):
            raise Exception(f'Failed to get results for task {new_task_id}') from response

        if response.json().get('success') is False:
            raise Exception(f'Task {new_task_id} failed to complete: {response.json().get("reason")}')

        self.result = response.json().get('result').get('data')

        return self

    def _api_request(self, method: str, endpoint: str, data: dict = None) -> Response or Exception:
        response = None

        for attempt in range(self.api_attempts):
            try:
                # Disable SSL warnings which are raised when using self-signed certificates
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

                from requests.api import request
                response = request(
                    method=method,
                    url=f'https://{self.api_host}:{self.api_port}/{endpoint}',
                    cert=self.api_ssl.get('pem') if self.api_ssl else None,
                    headers={
                        'Authorization': f'Bearer {self.api_token}' if self.api_token else None,
                    },
                    json=data or {},
                    verify=self.api_ssl.get('verify', False) if self.api_ssl else False
                )

                return response

            except Exception as ex:
                if self._retry_request(ex, response):
                    if attempt < self.api_attempts:
                        from time import sleep

                        sleep(1)
                        continue

                    else:
                        raise ex

                else:
                    raise ex


    @staticmethod
    def _retry_request(exception: Exception, response: Response) -> bool:
        """
        Determines if a request should be retried based on the exception type.

        Arguments
        exception: (Exception) The exception that occurred.
        response: (Response) The response object from the request.

        Returns
        (bool) True if the request should be retried, False otherwise.
        """
        if isinstance(exception, HTTPError):
            if response:
                if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
                    return True

        elif isinstance(exception, RETRYABLE_EXCEPTIONS):
            return True

        return False
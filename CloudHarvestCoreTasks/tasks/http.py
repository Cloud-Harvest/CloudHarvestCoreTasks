from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseTask
from typing import Literal


@register_definition(name='http', category='task')
class HttpTask(BaseTask):
    def __init__(self, url: str,
                 method: Literal['get', 'post', 'put', 'delete'] = 'get',
                 auth: dict = None,
                 cert: str = None,
                 data: dict = None,
                 content_type: str = 'application/json',
                 headers: dict = None,
                 verify: bool = True,
                 *args, **kwargs):

        """
        Initializes a new instance of the HttpTask class. This class is used to perform HTTP requests.
        """

        super().__init__(*args, **kwargs)

        self.url = url
        self.http_method = method.upper()       # cannot be named "method" because it conflicts with the method() function
        self.auth = auth
        self.cert = cert
        self.content_type = content_type
        self.data = data or {}
        self.headers = headers or {}
        self.verify = verify

        self.headers['User-Agent'] = f'CloudHarvest'


    def method(self, *args, **kwargs) -> 'BaseTask':
        """
        Executes the task.

        Returns:
            HttpTask: The current instance of the HttpTask class.
        """

        from requests import request
        from json import dumps

        # Perform the HTTP request
        response = request(
            method=self.http_method,
            url=self.url,
            auth=self.auth,
            data=dumps(self.data),
            headers=self.headers,
            cert=self.cert,
            verify=self.verify,
        )

        # Store the response in the result attribute
        self.result = response.json()

        return self

"""
This module contains the exceptions that are raised by the tasks in the CloudHarvestCoreTasks package.

"""
from .base import BaseTaskException


class DummyTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class ErrorTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class FileTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class HarvestRecordsetTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class JsonTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class MongoTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class PruneTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class RedisTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

class WaitTaskException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)

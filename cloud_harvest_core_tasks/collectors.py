from logging import getLogger
from .exceptions import BaseDataCollectionException
from .base import BaseTask, TaskStatusCodes

logger = getLogger('harvest')


class CacheDataCollector(BaseTask):
    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)

    def run(self):
        pass
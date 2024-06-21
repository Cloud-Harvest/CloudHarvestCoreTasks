from CloudHarvestCorePluginManager.decorators import register_definition
from logging import getLogger
from .exceptions import BaseDataCollectionException
from .base import BaseTask, TaskStatusCodes

logger = getLogger('harvest')


@register_definition('cache_data_collector')
class CacheDataCollector(BaseTask):
    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)

    def run(self):
        self.on_start()
        pass
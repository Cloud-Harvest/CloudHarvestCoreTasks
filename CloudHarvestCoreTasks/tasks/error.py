from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseTask
from CloudHarvestCoreTasks.exceptions import TaskError


@register_definition(name='error', category='task')
class ErrorTask(BaseTask):
    """
    The ErrorTask class is a subclass of the BaseTask class. It represents a task that raises an exception when run.
    This task is used for testing error handling in task chains and should not be used in production code. For example,
    this task is used for testing the `on: error` directive in task chain configurations.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def method(self):
        raise TaskError(self, 'This is an error task')

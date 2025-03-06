from typing import Literal, Any
from logging import getLogger
logger = getLogger('harvest')
_log_levels = Literal['debug', 'info', 'warning', 'error', 'critical']


class BaseHarvestException(BaseException):
    """
    Base exception class for all exceptions in the Harvest system
    """

    def __init__(self, prefix: str, *args, log_level: _log_levels = 'error'):
        super().__init__(*args)

        message = ' '.join([str(a) for a in args])

        getattr(logger, log_level.lower())(f'{prefix}: {message}')


class TaskChainException(BaseHarvestException):
    def __init__(self, task_chain: Any, *args):
        super().__init__(task_chain.id, *args)


class TaskException(BaseHarvestException):
    def __init__(self, task: Any, *args):

        if task.task_chain:
            prefix = f'{task.task_chain.id}[{task.task_chain.position + 1}]'

        else:
            prefix = task.name

        from CloudHarvestCoreTasks.tasks import TaskStatusCodes
        task.status = TaskStatusCodes.error

        super().__init__(prefix, *args)


class TaskTerminationException(TaskException):
    def __init__(self, *args):
        super().__init__(*args)

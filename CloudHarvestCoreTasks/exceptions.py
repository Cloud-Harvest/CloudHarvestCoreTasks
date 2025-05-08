from typing import Literal, Any
from logging import getLogger


logger = getLogger('harvest')
_log_levels = Literal['debug', 'info', 'warning', 'error', 'critical']


class BaseHarvestException(BaseException):
    """
    Base exception class for all exceptions in the Harvest system
    """

    def __init__(self, prefix: str, *args, log_level: _log_levels = 'error', **kwargs):
        message = format_args(*args)
        getattr(logger, log_level.lower())(f'{prefix}: {message}')

        super().__init__(message)


class TaskChainError(BaseHarvestException):
    def __init__(self, task_chain: Any, *args, **kwargs):
        if not isinstance(task_chain.errors, list):
            task_chain.errors = []

        task_chain.errors.append(args)

        super().__init__(task_chain.redis_name, *args, **kwargs)


class TaskError(BaseHarvestException):
    def __init__(self, task: Any, *args, **kwargs):

        # Format the arguments into something human-readable by recursively joining them into a string.
        formatted_args = format_args(*args)

        # Configure the log prefix
        if task.task_chain:
            prefix = f'{task.task_chain.id}[{task.task_chain.position + 1}]'

        else:
            prefix = task.name

        # Make sure the BaseTask.errors is a list (instantiated as None)
        if not isinstance(task.errors, list):
            task.errors = []

        # Append the error to the task's errors
        task.errors.append(formatted_args)

        from CloudHarvestCoreTasks.tasks import TaskStatusCodes
        task.status = TaskStatusCodes.error

        super().__init__(prefix, formatted_args, **kwargs)


class TaskTerminationError(TaskError):
    def __init__(self, *args):
        super().__init__(*args)


def format_args(*args):
    formatted_args = []
    for arg in args:
        if isinstance(arg, list):
            formatted_args.extend(arg)

        elif isinstance(arg, dict):
            for key, value in arg:
                formatted_args.append(f'{key}: {value}')

        else:
            formatted_args.append(str(arg))

    return ' '.join(formatted_args)
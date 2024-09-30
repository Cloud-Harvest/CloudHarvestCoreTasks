"""
This module defines the core classes and functionality for managing tasks and task chains in the Harvest system.

Classes:
    TaskStatusCodes (Enum): Defines the basic status codes for any given data collection object.
    TaskConfiguration: Manages the configuration of a task and provides methods to instantiate the task.
    BaseTask: Manages a single task in a task chain, providing the basic structure and methods for all tasks.
    BaseAuthenticationTask (BaseTask): Manages tasks related to authentication.
    BaseDataTask (BaseTask): Manages tasks that retrieve data from a data connection-based data provider.
    BaseTaskChain (List[BaseTask]): Manages a chain of tasks, providing methods to run, insert, and handle task states.
    BaseTaskPool: Manages a pool of tasks that can be executed concurrently.

Modules:
    CloudHarvestCorePluginManager.decorators: Provides decorators for registering task definitions.
    CloudHarvestCoreTasks.exceptions: Defines custom exceptions for the Harvest system.
    datetime: Provides classes for manipulating dates and times.
    enum: Provides support for enumerations.
    threading: Provides support for creating and managing threads.
    typing: Provides support for type hints.
    logging: Provides support for logging messages.
"""

from CloudHarvestCorePluginManager.decorators import register_definition
from datetime import datetime, timezone
from enum import Enum
from threading import Thread
from typing import List, Literal
from logging import getLogger


_log_levels = Literal['debug', 'info', 'warning', 'error', 'critical']
USER_FILTERS = {
    'add_keys': [],
    'count': False,
    'exclude_keys': [],
    'headers': [],
    'limit': None,
    'matches': [],
    'sort': None
}

logger = getLogger('harvest')


class TaskStatusCodes(Enum):
    """
    These are the basic status codes for any given Task object. Valid states are:
    - complete: The task has stopped and there are no more tasks to complete.
    - error: The task has stopped in an error state.
    - idle: The task is running but has no outstanding tasks.
    - initialized: The task has been created.
    - running: The task is currently processing data.
    - skipped: The task was skipped and did not run because a `when` condition was not met.
    - terminating: The task was ordered to stop and is currently attempting to shut down.
    """

    complete = 'complete'
    error = 'error'
    idle = 'idle'
    initialized = 'initialized'
    running = 'running'
    skipped = 'skipped'
    terminating = 'terminating'


class BaseTask:
    """
    The BaseTask class is responsible for managing a single task in a task chain. It provides the basic structure and
    methods that all tasks should have. BaseTask should not be instantiated directly, but should be inherited by
    subclasses that provide specific functionality.
    """

    # By default, the user filter class is HarvestRecordSetUserFilter. This is because most tasks which return data do
    # so after the overarching data set has been retrieved. An example of this include the FileTask which reads a file
    # and returns the data.
    USER_FILTER_STAGE = 'complete'

    def __init__(self,
                 name: str,
                 blocking: bool = True,
                 description: str = None,
                 on: dict = None,
                 task_chain: 'BaseTaskChain' = None,
                 result_as: str = None,
                 retry: dict = None,
                 user_filters:dict = None,
                 when: str = None,
                 with_vars: list[str] = None,
                 **kwargs):

        """
        BaseTask is the base class for all tasks in the Harvest project.
        It provides the basic structure and methods that all tasks should have.
        Tasks which inherit from BaseTask:
            - Must have a name attribute.
            - Are automatically added to the TaskRegistry.

        Attributes:
            name (str): The name of the task.
            blocking (bool): A boolean indicating whether the task is blocking or not. If True, the task will block the task chain until it completes.
            description (str): A brief description of what the task does.
            on (dict): A dictionary of task configurations for the task to run when it completes, errors, is skipped, or starts.
            task_chain (BaseTaskChain): The task chain that this task belongs to, if applicable.
            with_vars (list[str]): A list of variables that this task uses. These variables are retrieved from the task chain
                and used in TaskConfiguration which templates this task's class.
            status (TaskStatusCodes): The current status of the task.
            out_data (Any): The data that this task produces, if applicable.
            meta (Any): Any metadata associated with this task.
            retry (dict): A dictionary of retry configurations for the task.
            result_as (str): The name of the variable to store the result of this task in the task chain's variables.
            user_filters (dict): A dictionary of user filters to apply to the data.
                >>> user_filters = {
                >>>     'accepted': '*',                        # Regex pattern to match the filters allowed in this Task.
                >>>     'add_keys': ['new_key'],                # Keys to add to the data.
                >>>     'count': True,                          # Returns a count of data instead of the data itself.
                >>>     'exclude_keys': ['key_to_exclude'],     # Keys to exclude from the data.
                >>>     'headers': ['header1', 'header2'],      # Headers to include in the data. Also sets the header and sort order unless 'sort' is also provided.
                >>>     'limit': 10,                            # The maximum number of records to return.
                >>>     'matches': [['keyA=valueA', 'keyB=valueB'], 'keyA=valueC'],     # A list of matches to apply to the data.
                >>>     'sort': ['keyA', 'keyB:desc']           # The keys to sort the data by.
                >>> }
            when (str): A string representing a conditional argument using Jinja2 templating. If provided, the task
                will only run if the condition evaluates to True.
        """

        # Assigned attributes
        self.name = name
        self.blocking = blocking
        self.description = description
        self.on = on or {}
        self.output = None
        self.result_as = result_as
        self.retry = retry or {}
        self.task_chain = task_chain
        self.when = when
        self.with_vars = with_vars

        # Programmatic attributes
        self.attempts = 0
        self.status = TaskStatusCodes.initialized
        self.original_template = None
        self.result = None
        self.meta = None
        self.start = None
        self.end = None

        # Defaults < task-chain < user
        self.user_filters = USER_FILTERS | self.task_chain.user_filters if self.task_chain else {} | (user_filters or {})

    @property
    def duration(self) -> float:
        """
        Returns the duration of the task in seconds.
        """

        return ((self.end or datetime.now(tz=timezone.utc)) - self.start).total_seconds() if self.start else -1

    @property
    def position(self) -> int:
        """
        Returns the position of the task in the task chain. Position is determined by the order in which the task was
        instantiated and placed into the TaskChain's top-level List. It is possible for the position to change should a
        task insert new tasks ahead of the task calling this method.

        If the task is not part of a task chain, this method will return -1 as it is possible for a Task.position to be
        0 when it is the first Task in the chain.
        """

        try:
            return self.task_chain.index(self)

        except ValueError:
            return -1

    def apply_user_filters(self):
        """
        Applies user filters to the Task. The default user filter class is HarvestRecordSetUserFilter which is executed
        when on_complete() is called. This method should be overwritten in subclasses to provide specific functionality.
        """

        # If the user filters not configured for this Task, return
        if self.user_filters.get('accepted') is None:
            return

        from ..user_filters import HarvestRecordSetUserFilter

        with HarvestRecordSetUserFilter(recordset=self.result, **self.user_filters) as user_filter:
            self.result = user_filter.apply()

    def method(self):
        """
        This method should be overwritten in subclasses to provide specific functionality.
        """

        # Example code to simulate a long-running task.
        for i in range(10):

            # Make sure to include a block which handles termination
            if self.status == TaskStatusCodes.terminating:
                raise TaskTerminationException('Task was instructed to terminate.')

            from time import sleep
            sleep(1)

        # Set the data attribute to the result of the task, otherwise `as_result` will not populate.
        self.result = {'Test': 'Result'}

        return self

    def run(self) -> 'BaseTask':
        """
        Runs the task. This method will block until it completes, errors, or is terminated.

        Returns:
        BaseTask: The instance of the task.
        """

        try:
            max_attempts = self.retry.get('max_attempts') or 1

            while self.attempts < max_attempts:

                # Increment the number of attempts
                self.attempts += 1

                try:
                    self.on_start()

                    when_result = True

                    # Check of the `when` condition is met
                    if self.when and self.task_chain:
                        from tasks.templating import template_object
                        when_result = True if template_object(template={'result': '{{ ' + self.when + ' }}'}, variables=self.task_chain.variables).get('result') == 'True' else False

                    # If `self.when` condition is met or is None, run the method
                    if when_result:
                        self.method()

                    # Skip the task
                    else:
                        self.on_skipped()

                except Exception as ex:

                    # If the `retry` directive is provided, check if the task should be retried
                    if isinstance(self.retry, dict):
                        from re import findall, IGNORECASE

                        # Collect the retry conditions
                        retry = (
                            # Check if the error is in the retry directive
                            findall(self.retry.get('when_error_like') or '.*', str(ex.args), flags=IGNORECASE)
                            if self.retry.get('when_error_like') else True,

                            # Check if the error is not in the retry directive
                            not findall(self.retry.get('when_error_not_like') or '.*', str(ex.args), flags=IGNORECASE)
                            if self.retry.get('when_error_not_like') else True,

                            # CHeck if the number of attempts is less than the maximum number of attempts
                            self.attempts < max_attempts,

                            # Check if the task is not terminating
                            self.status != TaskStatusCodes.terminating
                        )

                        retry = all(retry)

                        # If any of the above conditions are met and the number of attempts is less than the maximum
                        # number of attempts, retry the task. Otherwise, call the on_error() method.
                        if retry:
                            from time import sleep
                            sleep(self.retry.get('delay_seconds') or 1.0)
                            continue

                        # If the task should not be retried, call the on_error() method
                        else:
                            self.on_error(ex)
                            break

                    # No retry directive was provided, call the on_error() method
                    else:
                        self.on_error(ex)
                        break

                else:
                    # If the task was not skipped, call the on_complete() method
                    if self.status != TaskStatusCodes.skipped:
                        self.on_complete()
                        break


        except Exception as ex:
            raise BaseTaskException(f'Top level error while running task {self.name}: {ex}')

        return self

    def _run_on_directive(self, directive: str):
        """
        Runs the task directive specified by the caller.

        Args:
            directive (str): The name of the method that called this method.

        Returns:
            BaseTask: The instance of the task.
        """

        i = 1

        from .factories import task_from_dict
        for d in (self.on.get(directive) or []):
            # If the task is blocking, insert the new task before the next task in the chain
            if self.blocking:
                self.task_chain.task_templates.insert(self.task_chain.position + i,
                                                      task_from_dict(task_configuration=d, task_chain=self.task_chain))

            # If the task is not blocking, append the new task to the end of the chain since the position of the current
            # task is not known.
            else:
                self.task_chain.task_templates.append(task_from_dict(task_configuration=d, task_chain=self.task_chain))

            i += 1

        return self

    def on_complete(self) -> 'BaseTask':
        """
        Method to run when a task completes.
        This method may be overridden in subclasses to provide specific completion logic.

        Returns:
            BaseTask: The instance of the task.
        """

        # Store the result in the task chain's variables if a result_as variable is provided
        if self.result_as and self.task_chain:
            self.task_chain.variables[self.result_as] = self.result

        if self.USER_FILTER_STAGE == 'complete':
            self.apply_user_filters()

        self._run_on_directive('complete')

        self.status = TaskStatusCodes.complete

        self.end = datetime.now(tz=timezone.utc)

        return self

    def on_error(self, ex: Exception) -> 'BaseTask':
        """
        Method to run when a task errors.
        This method may be overridden in subclasses to provide specific error handling logic.

        Args:
            ex (Exception): The exception that occurred.

        Returns:
            BaseTask: The instance of the task.
        """

        self.status = TaskStatusCodes.error

        if hasattr(ex, 'args'):
            self.meta = ex.args

        logger.error(f'Error running task {self.name}: {ex}')

        self._run_on_directive('error')

        return self

    def on_skipped(self) -> 'BaseTask':
        """
        Method to run when a task is skipped.
        This method may be overridden in subclasses to provide specific skip logic.

        Returns:
            BaseTask: The instance of the task.
        """

        self.status = TaskStatusCodes.skipped

        self._run_on_directive('skipped')

        return self

    def on_start(self) -> 'BaseTask':
        """
        Method to run when a task starts but before `method()` is called.
        This method may be overridden in subclasses to provide specific start logic.

        Returns:
            BaseTask: The instance of the task.
        """

        self.status = TaskStatusCodes.running
        self.start = datetime.now(tz=timezone.utc)

        self._run_on_directive('start')

        if self.USER_FILTER_STAGE == 'start':
            self.apply_user_filters()

        return self

    def terminate(self) -> 'BaseTask':
        """
        Terminates the task.
        This method may be overridden in subclasses to provide specific termination logic.

        Returns:
            BaseTask: The instance of the task.
        """

        self.status = TaskStatusCodes.terminating
        logger.warning(f'Terminating task {self.name}')

        return self


class BaseAuthenticationTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.auth = None

class BaseDataTask(BaseTask):
    """
    The BaseDataTask class is responsible for managing a task that retrieves data from a data connection-based data
    provider. A data provider could be a database, an API, or a cache. The BaseDataTask class provides the basic structure
    and methods that all data tasks should have. BaseDataTask may be instantiated directly or inherited by subclasses that
    provide specific functionality.
    """

    REQUIRED_CONFIGURATION_KEYS = []

    def __init__(self, command: str,
                 db: dict,
                 arguments: dict = None,
                 max_pool_size: int = 10,
                 *args, **kwargs):
        """
        Initializes a new instance of the BaseDataTask class. In order to instantiate a BaseDataTask, a configuration
        dictionary must be provided. This dictionary should contain the necessary information to connect to the data
        provider. Specific keys are identified in the REQUIRED_CONFIGURATION_KEYS attribute which is overridden in
        subclasses.

        Args:
            command (str): The command to run on the data provider.
            db (dict): The configuration for the data provider. The connection configuration is driver-specific
                       *except* when the 'alias' key is provided. In this case, the 'alias' key should be set to 'ephemeral'
                       or 'persistent' to connect to the appropriate data provider. Connections made using the 'alias' key
                       will leverage a Connection Pool if supported by the underlying data provider.
            arguments (dict, optional): Arguments to pass to the command.
            max_pool_size (int, optional): The maximum number of connections to allow in the connection pool. Defaults to 10.
        """

        super().__init__(*args, **kwargs)

        self._connection = None
        self._db = db or {}

        self.command = command
        self.arguments = arguments or {}
        self.max_pool_size = max_pool_size

        # Check if the configuration dictionary contains the required keys for the subclass data provider.
        # If the configuration dictionary contains an 'alias' key, check that 'alias' is either 'ephemeral' or 'persistent'.
        if db.get('alias'):
            if db['alias'] not in ('ephemeral', 'persistent'):
                raise BaseTaskException(f'Invalid alias value: {db["alias"]}. Must be "ephemeral" or "persistent".')

        # Check if the configuration dictionary contains the required keys for the subclass data provider.
        else:
            missing_keys = [key for key in self.REQUIRED_CONFIGURATION_KEYS if key not in db.keys()]
            if missing_keys:
                raise BaseTaskException(f'Missing required configuration keys: {", ".join(missing_keys)}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    @property
    def is_connected(self) -> bool:
        """
        Returns a boolean indicating whether the task is connected to the data provider.

        This method should be overwritten in subclasses to provide specific functionality.
        """

        return False

    def connect(self):
        """
        Connect the Task to the data provider.

        This method should be overwritten in subclasses to provide specific functionality. However, super().connect()
        should be called in the subclass method to ensure that the connection is established.
        """

        # If the task is already connected, return the connection
        if self.is_connected:
            return self._connection

    def disconnect(self):
        """
        Disconnect the task from the data provider.

        This method should be overwritten in subclasses to provide specific functionality.
        """

        pass


@register_definition(name='chain')
class BaseTaskChain(List[BaseTask]):
    """
    The BaseTaskChain class is responsible for managing a chain of tasks.

    It stores a list of tasks and provides methods to run the tasks in the chain, insert new tasks into the chain,
    and handle completion and error states. It also provides properties to track the progress of the task chain.

    Tasks are templated just before they are run. This allows for dynamic configuration of Tasks based on the variables
    provided by previous Tasks. The templating is done using the templating.functions.template_object function.

    Attributes:
        name (str): The name of the task chain.
        description (str): A brief description of what the task chain does.
        variables (dict): Variables that can be used by the tasks in the chain.
        task_templates (List[TaskConfiguration]): A list of task configurations for the tasks in the chain.
        status (TaskStatusCodes): The current status of the task chain.
        position (int): The current position in the task chain.
        start (datetime): The start time of the task chain.
        end (datetime): The end time of the task chain.
        _meta (Any): Any metadata associated with the task chain.

    Methods:
        detailed_progress() -> dict: Returns a dictionary representing the progress of the task chain.
        percent() -> float: Returns the current progress of the task chain as a percentage.
        total() -> int: Returns the total number of tasks in the task chain.
        find_task_position_by_name(task_name: str) -> int: Finds the position of a task in the task chain by its name.
        get_variables_by_names(*variable_names) -> dict: Retrieves variables stored in the 'BaseTaskChain.variables' property based on their names.
        insert_task_after_name(task_name: str, new_task_configuration: dict) -> 'BaseTaskChain': Inserts a new task into the task chain immediately after a task with a given name.
        insert_task_before_name(task_name: str, new_task_configuration: dict) -> 'BaseTaskChain': Inserts a new task into the task chain immediately before a task with a given name.
        insert_task_at_position(position: int, new_task_configuration: dict) -> 'BaseTaskChain': Inserts a new task into the task chain at a specific position.
        on_complete() -> 'BaseTaskChain': Method to run when the task chain completes.
        on_error(ex: Exception) -> 'BaseTaskChain': Method to run when the task chain errors.
        run() -> 'BaseTaskChain': Runs the task chain.
        terminate() -> 'BaseTaskChain': Terminates the task chain.
    """

    def __init__(self,
                 template: dict,
                 cache_progress: bool = False,
                 user_filters: dict = None,
                 *args, **kwargs):
        """
        Initializes a new instance of the BaseTaskChain class.

        Args:
            template(dict): The configuration for the task chain.
                name(str): The name of the task chain.
                tasks(List[dict]): A list of task configurations for the tasks in the chain.
                description(str, optional): A brief description of what the task chain does. Defaults to None.
                max_workers(int, optional): The maximum number of concurrent workers that are permitted.
            cache_progress(bool, optional): A boolean indicating whether the progress of the task chain should
                                            be reported to the Ephemeral Silo. Defaults to False.
        """
        self.original_template = template

        super().__init__()

        from uuid import uuid4
        self.id = str(uuid4())

        self.name = template['name']
        self.description = template.get('description')
        self.cache_progress = cache_progress

        self.variables = {}
        self.task_templates: List[dict or BaseTask] = template.get('tasks', [])

        self.status = TaskStatusCodes.initialized
        self.pool = BaseTaskPool(chain=self,
                                 max_workers=template.get('max_workers', 4),
                                 idle_refresh_rate=template.get('idle_refresh_rate', 3),
                                 worker_refresh_rate=template.get('worker_refresh_rate', .5)).start()

        self.position = 0

        self.start = None
        self.end = None
        self.user_filters = USER_FILTERS | (user_filters or {})

        self._data = None
        self._meta = None

    def __enter__(self) -> 'BaseTaskChain':
        """
        This method is called when the context management protocol is initiated using the 'with' statement.
        It returns the instance of the task chain itself.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        This method is called when the context management protocol is ended (when exiting the 'with' block).
        It doesn't do anything in this case.

        Args:
            exc_type (type): The type of the exception that caused the context management protocol to end, if any.
            exc_val (Exception): The instance of the exception that caused the context management protocol to end, if any.
            exc_tb (traceback): A traceback object encapsulating the call stack at the point where the exception was raised, if any.
        """

        return None

    @property
    def data(self):
        """
        Returns the data produced by the last task in the task chain.
        """

        return self.variables.get('result')

    @property
    def detailed_progress(self) -> dict:
        """
        This method calculates and returns the progress of the task chain.

        It returns a dictionary with the following keys:
        - 'total': The total number of tasks in the task chain.
        - 'current': The current position of the task chain.
        - 'percent': The percentage of tasks completed in the task chain.
        - 'duration': The total duration of the task chain in seconds. If the task chain has not started, it returns 0.
        - 'counts': A dictionary with the count of tasks in each status. The keys of this dictionary are the status codes defined in the TaskStatusCodes Enum.

        Returns:
            dict: A dictionary representing the progress of the task chain.
        """

        from datetime import datetime, timezone
        count_result = {
            k: 0 for k in TaskStatusCodes
        }

        # iterate over tasks to get their individual status codes
        for task in self:
            count_result[task.status] += 1

        return {
            'total': self.total,
            'current': self.position,
            'percent': (self.position / self.total) * 100,
            'duration': (self.end or datetime.now(tz=timezone.utc) - self.start).total_seconds() if self.start else 0,
            'counts': count_result
        }

    @property
    def percent(self) -> float:
        """
        Returns the current progress of the task chain as a percentage cast as float.
        """

        return self.position / self.total if self.total > 0 else -1

    @property
    def performance_metrics(self) -> List[dict]:
        """
        This method calculates and returns the performance metrics of the task chain.

        The performance metrics include information about each task in the task chain, such as its position, name,
        status, data size, duration, start and end times. It also includes system metrics like CPU and memory usage,
        and disk space.

        The method returns a dictionary with the following keys:

        - 'TaskMetrics': Contains a list of dictionaries, each representing a task in the task chain. Each dictionary
          includes the following keys:
            - 'Position': The position of the task in the task chain.
            - 'Name': The name of the task.
            - 'Status': The status of the task.
            - 'DataBytes': The size of the data produced by the task, in bytes.
            - 'Records': The number of records in the task's data, if applicable.
            - 'Duration': The duration of the task, in seconds.
            - 'Start': The start time of the task.
            - 'End': The end time of the task.

        - 'Timings': Contains statistics about the durations of the tasks in the task chain, including the average,
           maximum, minimum, and standard deviation.

        - 'SystemMetrics': Contains a list of dictionaries, each representing a system metric. Each dictionary includes
          the following keys:
            - 'Name': The name of the metric.
            - 'Value': The value of the metric.

            The system metrics returned are:
            - CPU Cores: The number of CPU cores.
            - CPU Threads: The number of CPU threads.
            - CPU Architecture: The CPU architecture.
            - CPU Clock Speed: The CPU clock speed.
            - OS Architecture: The OS architecture.
            - OS Version: The OS version.
            - OS 64/32 bit: Whether the OS is 64 or 32 bit.
            - OS Runtime: The OS runtime.
            - Total Memory: The total memory available.
            - Available Memory: The available memory.
            - Swap Size: The size of the swap space.
            - Swap Usage: The amount of swap space used.
            - Total Disk Space: The total disk space.
            - Used Disk Space: The used disk space.
            - Free Disk Space: The free disk space.

        Returns:
            List[dict]: A dictionary representing the performance metrics of the task chain.
        """

        from sys import getsizeof

        # This part of the report returns results for each task in the task chain.
        task_metrics = [
            {
                'Position': self.position,
                'Name': task.name,
                'Status': task.status.__str__(),
                'Attempts': task.attempts,
                'DataBytes': getsizeof(task.result),
                'Records': len(task.result) if hasattr(task.result, '__len__') else 'N/A',
                'Duration': task.duration,
                'Start': task.start,
                'End': task.end,
            }
            for task in self
        ]

        # Gather metrics for the entire task chain
        # We generate multiple lists through one loop to perform the necessary calculations without having to loop
        # multiple times through the TaskChain's tasks.
        total_records = []
        total_result_size = []
        starts = []
        ends = []
        for task in self:
            if hasattr(task.result, '__len__'):
                total_records.append(len(task.result))

            total_result_size.append(getsizeof(task.result))
            starts.append(task.start)
            ends.append(task.end)

        # Add a total row to the task metrics
        total_records = sum(total_records)
        total_result_size = sum(total_result_size)
        starts = min(starts)
        ends = max(ends)

        task_metrics.append({
            'Position': 'Total',
            'Name': '',
            'Status': self.status.__str__(),
            'Records': total_records,
            'DataBytes': total_result_size,
            'Duration': (ends - starts).total_seconds() if starts and ends else 0,
            'Start': starts,
            'End': ends,
        })

        # Add a buffer run between the task list and the Total
        # We add it at this stage just in case there are no Tasks in the TaskChain
        # which means the only row in the task_metrics list is the Total row
        task_metrics.insert(-1, {k: '' for k in task_metrics[-1].keys()})

        return [
            {
                'data': task_metrics,
                'meta': {
                    'headers': [k for k in task_metrics[0].keys()]
                }
            }
        ]

    @property
    def result(self) -> dict:
        """
        Returns the result of the task chain. This can be interpreted either as the 'result' variable in the task
        chain's variables or the data and meta of the last task in the chain.
        """

        result = {}

        try:
            if self.status == TaskStatusCodes.initialized:
                result = {
                    'info': 'The task chain has not been run yet.'
                }

            else:
                result = {
                    'data': self._data or self.data or self[-1].result,
                    'meta': self._meta or self[-1].meta
                }

        except IndexError:
            result = {
                'error': ' '.join([f'None of the {len(self.task_templates)} tasks in the task chain `{self.name}` were'
                                   f' successfully instantiated. You may have a configuration error.',
                                   str(self._meta) or 'No error message provided.'])
            }

        finally:
            return result

    @property
    def total(self) -> int:
        """
        Returns the total number of tasks in the task chain.
        """

        return len(self.task_templates)

    def find_task_by_name(self, task_name: str) -> 'BaseTask':
        """
        This method finds a task in the task chain by its name.

        Args:
            task_name (str): The name of the task to find.

        Returns:
            BaseTask: The task with the given name.
        """

        for task in self:
            if task.name == task_name:
                return task

    def find_task_position_by_name(self, task_name: str) -> int:
        """
        This method finds the position of a task in the task chain by its name.

        Args:
            task_name (str): The name of the task to find.

        Returns:
            int: The position of the task in the task chain. If the task is not found, it returns None.
        """

        for position, task in enumerate(self.task_templates):
            if task.get('name') == task_name:
                return position

    def get_variables_by_names(self, *variable_names) -> dict:
        """
        Retrieves variables stored in the 'BaseTaskChain.variables' property based on their names (top level key).
        Note that the values

        Args:
            *variable_names: The variables to return.

        Returns: a dictionary of variable names (keys) and their values.
        """

        return {
            key: value
            for key, value in self.variables.items()
            if key in variable_names
        }

    def insert_task_after_name(self, task_name: str, new_task_configuration: dict or BaseTask) -> 'BaseTaskChain':
        """
        This method inserts a new task into the task chain immediately after a task with a given name.

        Args:
            task_name (str): The name of the task after which the new task should be inserted.
            new_task_configuration (dict): The configuration of the new task to be inserted.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        self.task_templates.insert(self.find_task_position_by_name(task_name) + 1, new_task_configuration)

        return self

    def insert_task_before_name(self, task_name: str, new_task_configuration: dict or BaseTask) -> 'BaseTaskChain':
        """
        This method inserts a new task into the task chain immediately before a task with a given name.

        Args:
            task_name (str): The name of the task before which the new task should be inserted.
            new_task_configuration (dict): The configuration of the new task to be inserted.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        position = self.find_task_position_by_name(task_name)

        if position < self.position:
            raise BaseTaskException('Cannot insert a task before the current task.')

        else:
            self.task_templates.insert(position - 1, new_task_configuration)

        return self

    def insert_task_at_position(self, position: int, new_task_configuration: dict or BaseTask) -> 'BaseTaskChain':
        """
        This method inserts a new task into the task chain at a specific position.

        Args:
            position (int): The position at which the new task should be inserted.
            new_task_configuration (dict): The configuration of the new task to be inserted.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        if position > self.total:
            self.task_templates.append(new_task_configuration)

        else:
            self.task_templates.insert(position, new_task_configuration)

        return self

    def on_complete(self) -> 'BaseTaskChain':
        """
        Method to run when the task chain completes.
        This method may be overridden in subclasses to provide specific completion logic.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        self.status = TaskStatusCodes.complete
        self.end = datetime.now(tz=timezone.utc)

        return self

    def on_error(self, ex: Exception) -> 'BaseTaskChain':
        """
        Method to run when the task chain errors.
        This method may be overridden in subclasses to provide specific error handling logic.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        self.status = TaskStatusCodes.error
        self._meta = ex.args

        if self.pool.queue_size:
            self.pool.terminate()

        logger.error(f'Error running task chain {self.name}: {ex}')

        return self

    def on_start(self) -> 'BaseTaskChain':
        """
        Method to run when the task chain starts.
        This method may be overridden in subclasses to provide specific start logic.
        """

        self.status = TaskStatusCodes.running
        self.start = datetime.now(tz=timezone.utc)

        return self

    def run(self) -> 'BaseTaskChain':
        """
        Runs the task chain. This method will block until all tasks in the chain are completed.
        Note that this method may be overwritten in subclasses to provide specific functionality.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        # Kick off the Ephemeral Silo thread if the cache_progress flag is set
        ephemeral_cache_thread = self.update_task_chain_cache_thread() if self.cache_progress else None

        try:
            self.on_start()
            self.position = 0

            while True:
                # Instantiate the task from the task configuration
                try:
                    from .factories import task_from_dict
                    task = task_from_dict(task_configuration=self.task_templates[self.position],
                                          task_chain=self,
                                          template_vars=self.variables)

                # Break when there are no more tasks to run
                except IndexError:
                    break

                self.append(task)

                # Execute the task
                if task.blocking:
                    task.run()

                # Add it to the pool to be run asynchronously
                else:
                    self.pool.add(task)

                # Check for termination
                if self.status == TaskStatusCodes.terminating:
                    raise TaskTerminationException('Task chain was instructed to terminate.')

                # Hold within the loop if there are outstanding pool tasks because the async task might have an
                # on_* directive which needs to be added and processed. By waiting here, we ensure that the task chain
                # will not complete until all tasks have been processed.
                if self.pool.queue_size > 0 and len(self.task_templates) == len(self):
                    self.pool.wait_until_complete()

                # Increment the position
                self.position += 1

            if self.pool.queue_size > 0:
                self.pool.wait_until_complete()

        except Exception as ex:
            self.on_error(ex)

        finally:
            self.on_complete()

            if ephemeral_cache_thread:
                ephemeral_cache_thread.join(timeout=5)

            return self

    def terminate(self) -> 'BaseTaskChain':
        """
        Terminates the task chain.
        This method may be overridden in subclasses to provide specific termination logic.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        self.status = TaskStatusCodes.terminating

        return self

    def update_task_chain_cache_thread(self) -> Thread:
        """
        This method is responsible for updating the job cache with the task chain's progress.
        """

        def update_task_chain_cache():
            """
            Updates the job cache with the task chain's progress.
            """

            from silos.ephemeral import connect

            while True:
                cache_entry = {
                                  'id': self.id,
                                  'status': self.status.__str__(),
                                  'start': self.start,
                                  'end': self.end,
                              } | self.detailed_progress

                try:
                    client = connect(database='chains')

                    client.hset(name=self.id, mapping=cache_entry)

                    # A job which has not updated in 15 minutes is considered stale and will be removed from the cache.
                    client.expire(name=self.id, time=900)

                except Exception as ex:
                    logger.error(f'{self.name}: Error updating job cache: {ex}')

                finally:
                    from time import sleep

                    match self.status:
                        case TaskStatusCodes.initialized, TaskStatusCodes.idle:
                            sleep(5)

                        case _:
                            sleep(1)

        thread = Thread(target=update_task_chain_cache, daemon=True)
        thread.start()

        return thread


class BaseTaskPool:
    """
    The BaseTaskPool class is responsible for managing a pool of tasks that can be executed concurrently. Unlike the
    ThreadPoolExecutor provided by concurrent.futures, the BaseTaskPool class is designed to continue working even if
    the Pool's queue is empty. This allows for the addition of new tasks to the pool while it is running.

    TaskChains should call terminate() on the TaskPool to stop the pool from running once all the Chain's Tasks
    have completed. This will prevent the pool from running indefinitely.

    Attributes:
        max_workers (int): The maximum number of concurrent workers.
        worker_refresh_rate (float): The rate at which the pool checks for task completion and starts new tasks.
        idle_refresh_rate (float): The rate at which the pool checks for new tasks when idle.
        _pool (list): The list of tasks waiting to be executed.
        _active (list): The list of tasks currently being executed.
        _complete (list): The list of tasks that have completed execution.
        _minder_thread (Thread): The thread responsible for managing the task pool.
        status (TaskStatusCodes): The current status of the task pool.
    """

    def __init__(self, chain: BaseTaskChain, max_workers: int, idle_refresh_rate: float = 3, worker_refresh_rate: float = .5):
        """
        Initializes a new instance of the BaseTaskPool class.

        Args:
            max_workers (int): The maximum number of concurrent workers.
            idle_refresh_rate (float, optional): The rate at which the pool checks for new tasks when idle. Defaults to 3 seconds.
            worker_refresh_rate (float, optional): The rate at which the pool checks for task completion and starts new tasks. Defaults to 0.5 seconds.
        """

        self.chain = chain
        self.max_workers = max_workers
        self.worker_refresh_rate = worker_refresh_rate
        self.idle_refresh_rate = idle_refresh_rate

        self._pool = []         # List of tasks waiting to be executed
        self._active = []       # List of tasks currently being executed
        self._complete = []     # List of tasks that have completed execution

        from threading import Thread
        self._minder_thread = Thread(target=self._worker, daemon=True)  # Thread to manage the task pool

        self.status = TaskStatusCodes.initialized  # Initial status of the task pool

    @property
    def queue_size(self) -> int:
        """
        Returns the number of pending and running tasks in the pool.
        """

        return len(self._active) + len(self._pool)

    def add(self, task: BaseTask) -> 'BaseTaskPool':
        """
        Adds a task to the pool.

        Args:
            task (BaseTask): The task to be added to the pool.
        """

        self._pool.append(task)
        return self

    def wait_until_complete(self, timeout: float = 0) -> 'BaseTaskPool':
        """
        Waits until all tasks in the pool have completed.

        Args:
            timeout (float, optional): The maximum number of seconds to wait for the tasks to complete.
                                       If 0, the method will wait indefinitely. Defaults to 0.
        """

        from time import sleep
        from datetime import datetime

        wait_start = datetime.now()

        while self.queue_size > 0:
            if timeout != 0:
                if (datetime.now() - wait_start).total_seconds() > timeout:
                    break

            sleep(1)

        return self

    def remove(self, task: BaseTask) -> 'BaseTaskPool':
        """
        Removes a task from the pool.

        Args:
            task (BaseTask): The task to be removed from the pool.
        """

        pool = self._find_task(task)
        try:
            pool.remove(task)

        except ValueError:
            pass  # Task not found in the pool

        return self

    def start(self) -> 'BaseTaskPool':
        """
        Starts the minder thread to manage the task pool.
        """

        self._minder_thread.start()
        return self

    def terminate(self) -> 'BaseTaskPool':
        """
        Terminates the task pool.
        """

        self.status = TaskStatusCodes.terminating

        # Terminate all tasks in the pool
        for task in self._pool + self._active:
            task.terminate()

        # Wait for the minder thread to finish
        self._minder_thread.join()
        return self

    def _worker(self) -> None:
        """
        The method run by the minder thread to manage task execution.
        """

        from time import sleep
        from threading import Thread

        self.status = TaskStatusCodes.running

        while True:
            if len(self._active) < self.max_workers and self._pool:
                next_task = self._pool.pop(0)  # Get the next task from the pool
                self._active.append(next_task)  # Add the task to the active list

                Thread(target=next_task.run).start()  # Start the task in a new thread

            for task in self._active:
                if task.status in (TaskStatusCodes.complete, TaskStatusCodes.error, TaskStatusCodes.skipped):
                    self._active.remove(task)
                    self._complete.append(task)

            # Wait before checking the task statuses again
            if self.queue_size:
                sleep(self.worker_refresh_rate)
            else:
                if self.status == TaskStatusCodes.terminating:
                    break
                else:
                    sleep(self.idle_refresh_rate)

    def _find_task(self, task: BaseTask) -> list:
        """
        Finds the pool (waiting, active, or complete) that contains the given task.

        Args:
            task (BaseTask): The task to find.

        Returns:
            list: The pool that contains the task.
        """

        for pool in [self._pool, self._active, self._complete]:
            if task in pool:
                return pool

        return []


class BaseHarvestException(BaseException):
    """
    Base exception class for all exceptions in the Harvest system
    """

    def __init__(self, *args, log_level: _log_levels = 'error'):
        super().__init__(*args)

        getattr(logger, log_level.lower())(str(args))


class BaseTaskException(BaseHarvestException):
    def __init__(self, *args):
        super().__init__(*args)


class TaskTerminationException(BaseTaskException):
    def __init__(self, *args):
        super().__init__(*args)
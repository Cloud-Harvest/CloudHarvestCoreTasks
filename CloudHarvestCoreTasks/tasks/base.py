from datetime import datetime, timezone
from typing import Any, List, Literal

from logging import getLogger
logger = getLogger('harvest')


class TaskStatusCodes:
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

    @classmethod
    def get_codes(cls):
        return [
            attr for attr in dir(cls)
            if not attr.startswith('__') and not callable(getattr(cls, attr))
        ]


class BaseTask:
    """
    The BaseTask class is responsible for managing a single task in a task chain. It provides the basic structure and
    methods that all tasks should have. BaseTask should not be instantiated directly, but should be inherited by
    subclasses that provide specific functionality.
    """

    def __init__(self,
                 name: str,
                 blocking: bool = True,
                 data: Any = None,
                 description: str = None,
                 ignore_filters: bool = False,
                 iterate: dict = None,
                 on: dict = None,
                 task_chain: Any = None,
                 result_as: (dict or str) = None,
                 retry: dict = None,
                 filters:dict = None,
                 when: str = None,
                 **kwargs):

        """
        BaseTask is the base class for all tasks in the Harvest project.
        It provides the basic structure and methods that all tasks should have.
        Tasks which inherit from BaseTask:
            - Must have a name attribute.
            - Are automatically added to the TaskRegistry.

        Arguments:
            name (str): The name of the task.
            blocking (bool): A boolean indicating whether the task is blocking or not. If True, the task will block the task chain until it completes.
            description (str): A brief description of what the task does.
            ignore_filters (bool): A boolean indicating whether to ignore user filters or not.
            iterate (str): A variable to iterate over.
            on (dict): A dictionary of task configurations for the task to run when it completes, errors, is skipped, or starts.
                >>> on = {
                >>>     'complete': [TaskConfiguration],
                >>>     'error': [TaskConfiguration],
                >>>     'skipped': [TaskConfiguration],
                >>>     'start': [TaskConfiguration]
                >>> }
            task_chain (chains.base.BaseTaskChain): The task chain that this task belongs to, if applicable.
            retry (dict): A dictionary of retry configurations for the task.
                >>> retry = {
                >>>     'delay_seconds': 1.0,                  # The number of seconds to delay before retrying the task.
                >>>     'max_attempts': 3,                     # The maximum number
                >>>     'when_error_like': '.*',               # A regex pattern to match the error message.
                >>>     'when_error_not_like': '.*',           # A regex pattern to match the error message.
                >>> }
            result_as (str or dict): The name of the variable to store the result of this task in the task chain's variables.
                >>> result_as = 'variable_name'
                >>> result_as = {
                >>>     'name': 'variable_name',
                >>>     'mode': 'append', 'extend', 'merge', 'overwrite'    # The mode to store the result in the variable. 'overwrite' is the default.
                >>> }
            filters (dict): A dictionary of user filters to apply to the data.
                >>> filters = {
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
                >>> when: {{ var.variable_name == "value" }}
        """

        # Assigned attributes
        self.name = name
        self.blocking = blocking
        self.data = data
        self.description = description
        self.ignore_filters = ignore_filters
        self.iterate = iterate or {}
        self.on = on or {}
        self.output = None
        self.result_as = result_as
        self.retry = retry or {}
        from CloudHarvestCoreTasks.chains.base import BaseTaskChain
        self.task_chain: BaseTaskChain = task_chain
        self.when = when

        # Programmatic attributes
        self.attempts = 0
        self.status = TaskStatusCodes.initialized
        self.original_template = None
        self.result = None
        self.meta = {
            'Errors': []
        }
        self.start = None
        self.end = None

        # Defaults < user
        self.filters = filters

    @property
    def duration(self) -> float:
        """
        Returns the duration of the task in seconds.
        """

        return ((self.end or datetime.now(tz=timezone.utc)) - self.start).total_seconds() if self.start else -1

    @property
    def errors(self) -> List[str]:
        """
        Returns a list of errors that occurred during the task.
        """

        return self.meta.get('Errors')

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

    def apply_filters(self) -> 'BaseTask':
        """
        Applies user filters to the Task. The default user filter class is HarvestRecordSetUserFilter which is executed
        when on_complete() is called. This method should be overwritten in subclasses to provide specific functionality.
        """
        return self

    def method(self, *args, **kwargs) -> 'BaseTask':
        """
        This method should be overwritten in subclasses to provide specific functionality.
        """

        # Example code to simulate a long-running task.
        for i in range(10):

            # Make sure to include a block which handles termination
            if self.status == TaskStatusCodes.terminating:
                break

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
        from CloudHarvestCoreTasks.exceptions import TaskException

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
                        from CloudHarvestCoreTasks.templating import template_object
                        when_result = True if template_object(template={'result': '{{ ' + self.when + ' }}'},
                                                              variables=self.task_chain.variables).get('result') == 'True' else False

                    # If `self.when` condition is met or is None, run the method
                    if when_result:
                        self.method()

                    # Skip the task
                    else:
                        self.on_skipped()

                except (Exception, TaskException) as ex:
                    # If the `retry` directive is provided, check if the task should be retried. We include isinstance()
                    # to ensure that the retry directive is a dictionary.
                    if self.retry and isinstance(self.retry, dict):
                        from re import findall, IGNORECASE

                        # Collect the retry conditions
                        retry = (
                            # Check if the error is in the retry directive
                            findall(self.retry.get('when_error_like') or '.*', str(ex.args), flags=IGNORECASE)
                            if self.retry.get('when_error_like') else True,

                            # Check if the error is not in the retry directive
                            not findall(self.retry.get('when_error_not_like') or '.*', str(ex.args), flags=IGNORECASE)
                            if self.retry.get('when_error_not_like') else True,

                            # Check if the number of attempts is less than the maximum number of attempts
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

                        # If the result is a generator, convert it to a list. We do this at this stage instead of
                        # inside the on_complete() method to make sure any post-task processing will be handled on the
                        # entire data result instead against a generator which may not be accessible following the
                        # completion of self.method(). Additionally, on_complete() can be overwritten so it is possible
                        # this crucial step may be missed.

                        from types import GeneratorType
                        if isinstance(self.result, GeneratorType):
                            self.result = [r for r in self.result]

                        self.on_complete()
                        break


        except Exception as ex:
            raise TaskException(self, ex)

        finally:
            # Update the metadata with the task's status, duration, and other information
            self.meta |= {
                'attempts': self.attempts,
                'count': len(self.result) if hasattr(self, '__len__') else 1,
                'duration': self.duration,
                'status': self.status
            }
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

        for d in (self.on.get(directive) or []):
            # If the task is blocking, insert the new task before the next task in the chain
            if self.blocking:
                self.task_chain.task_templates.insert(self.task_chain.position + i, d)

            # If the task is not blocking, append the new task to the end of the chain since the position of the current
            # task is not known.
            else:
                self.task_chain.task_templates.append(d)

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

        # Run the on_complete directive
        self._run_on_directive('complete')

        # Update the end time of the task
        self.end = datetime.now(tz=timezone.utc)

        # Update the status of the task
        self.status = TaskStatusCodes.complete

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
            self.meta['Errors'].append(str(ex.args))

        if self.task_chain:
            logger.error(f'{self.task_chain.id}[{self.position + 1}]: Error running task "{self.name}": {ex}')

        else:
            logger.error(f'Error running task "{self.name}": {ex}')

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


class BaseDataTask(BaseTask):
    """
    The BaseDataTask class is responsible for managing a task that retrieves data from a data connection-based data
    provider. A data provider could be a database, an API, or a cache. The BaseDataTask class provides the basic structure
    and methods that all data tasks should have. BaseDataTask may be instantiated directly or inherited by subclasses that
    provide specific functionality.
    """

    # The connection key map is used to map the connection attributes to the appropriate attributes in the subclass.
    # base_configration_key: The attribute in the BaseDataTask class.
    # driver_configuration_key: The attribute specific to the data provider driver / module.
    # Format: (base_configuration_key, driver_configuration_key)
    CONNECTION_KEY_MAP = (
        ('host', 'host'),
        ('port', 'port'),
        ('username', 'username'),
        ('password', 'password'),
        ('database', 'database')
    )

    # Connection pools are used to store connections to the data provider. This reduces the number of connections to the
    # data provider and allows us to reuse connections. Override this attribute in subclasses to provide data provider
    # specific connection pools.
    CONNECTION_POOLS = {}

    # The required configuration keys are used to validate the configuration of the task. All keys in this tuple must
    # be provided for the Task to be considered valid. Override this attribute in subclasses to provide data provider
    # specific configuration keys.
    REQUIRED_CONFIGURATION_KEYS = ()

    def __init__(self, command: str,
                 silo: str,
                 arguments: dict = None,
                 *args, **kwargs):
        """
        Initializes a new instance of the BaseDataTask class. In order to instantiate a BaseDataTask, a configuration
        dictionary must be provided. This dictionary should contain the necessary information to connect to the data
        provider. Specific keys are identified in the REQUIRED_CONFIGURATION_KEYS attribute which is overridden in
        subclasses.

        Typically, 'silo' or the host configuration is required. If 'silo' is provided, the configuration is retrieved
        from the stored Silos dictionary. 'silo' always takes precedence over the host configuration.

        Args:
            command (str): The command to run on the data provider.
            arguments (dict, optional): Arguments to pass to the command.
            silo (str, optional): The name of the silo to use for the task. Defaults to None.
        """

        # Initialize the BaseTask class
        super().__init__(*args, **kwargs)

        from CloudHarvestCoreTasks.silos import get_silo

        # Assigned attributes
        self.silo = get_silo(silo)
        self.arguments = arguments or {}
        self.command = command

        # Programmatic attributes
        self.calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    @property
    def base_command_part(self):
        """
        Extracts the actual command from 'self.command' and returns it while preserving the path of the original command.
        """

        result = self.command

        if '.' in self.command:
            # Extract the command from the string
            result = self.command.split('.')[0]

        elif '[' and ']' in self.command:
            # Extract the command from the string
            result = self.command.split('[')[0]

        return result

    def walk_result_command_path(self, result: Any) -> Any:
        """
        Walks the command path and returns the result, if applicable.

        >>> # The Task Configuration supplies the command 'find.row_count'. row_count is a property of the find command.
        >>> self.command = 'find.row_count'
        >>> # The find command returns CursorType() which is stored in the variable 'result'.
        >>> result = CursorType()
        >>> # The walk_result_command_path() method will walk the command path and return CursorType().row_count.
        >>> self.walk_result_command_path(result)
        >>> # The final result is returned.
        >>> 10
        """

        if '.' in self.command or ('[' and ']') in self.command:
            # Walk the command path and return the result, if applicable
            self.task_chain.variables[self.base_command_part] = result

            # Walks the command path and returns the result. This allows commands such as MongoDb's 'find.row_count'.
            from CloudHarvestCoreTasks.factories import replace_variable_path_with_value
            result: Any = replace_variable_path_with_value(original_string=f'var.{self.command}',
                                                           task_chain=self.task_chain,
                                                           fail_on_unassigned=True)

            # Removes the command from the variables
            self.task_chain.variables.pop(self.base_command_part)

        return result


class BaseFilterableTask(BaseTask):
    """
    The BaseFilterableTask class is a subclass of the BaseTask class and is used to manage a task that can be filtered
    based on user-defined filters.

    Attributes:
        filters (dict): A dictionary of user filters to apply to the data.
    """

    def __init__(self,
                 filters: str = None,
                 order_of_operations: List[str] = None,
                 add_keys: List[str] = None,
                 count: bool = False,
                 exclude_keys: List[str] = None,
                 headers: List[str] = None,
                 limit: int = None,
                 matches: List[List[str]] = None,
                 sort: List[str] = None,
                 *args, **kwargs
                 ):
        """
        Initializes a new instance of the BaseFilterableTask class.

        Args:

        """

        super().__init__(*args, **kwargs)

        # The accepted filter is a regular expression that determines which filters are accepted by the task. If a filter
        # is not accepted, it will be ignored. To accept all filters, provide a string like '.*'. To accept no filters,
        # provide None (default)
        self.filters = filters

        # The default order of operations for all filters. This order is used to ensure that the filters are applied in an
        # optimal and consistent manner.
        self.order_of_operations = order_of_operations or (
            'add_keys',     # Need all possible keys for matching and sorting
            'matches',      # Filter the data
            'sort',         # Sort the data
            'limit',        # Limit the data
            'exclude_keys', # Exclude keys from the data
            'headers',      # Set the headers of the data
            'count'         # Return a count of the data
        )

        # Sets the values
        self.add_keys = self.set_accepted_filters('add_keys', add_keys, [])
        self.count = self.set_accepted_filters('count', count, False)
        self.exclude_keys = self.set_accepted_filters('exclude_keys', exclude_keys, [])
        self.headers = self.set_accepted_filters('headers', headers, [])
        self.limit = self.set_accepted_filters('limit', limit, None)
        self.matches = self.set_accepted_filters('matches', matches, [])
        self.sort = self.set_accepted_filters('sort', sort, [])

    def set_accepted_filters(self, filter_name: str, filter_value: Any, default_value: Any) -> Any:
        if self.filters is None:
            return default_value

        from re import compile
        filters = compile(self.filters)

        return filter_value or default_value if filters.match(filter_name) else default_value

    def apply_filters(self) -> 'BaseFilterableTask':
        """
        This method applies the user filters to the data based on the ORDER_OF_OPERATIONS.
        """

        for operation in self.order_of_operations:
            getattr(self, f'_filter_{operation}')()

        return self

    def filter_keys(self) -> List[str]:
        """
        This method returns the expected keys of the data based on the provided headers, add_keys, and exclude_keys.

        Returns
        A list of the headers of the data.
        """

        # If the task chain has headers, we use them as the default headers for the task
        if hasattr(self.task_chain, 'headers'):
            headers = self.headers or self.task_chain.headers

        else:
            headers = self.headers

        return [
            header for header in (headers + self.add_keys)
            if header not in self.exclude_keys
        ]

    def _filter_add_keys(self, *args, **kwargs):
        pass

    def _filter_count(self, *args, **kwargs):
        pass

    def _filter_exclude_keys(self, *args, **kwargs):
        pass

    def _filter_headers(self, *args, **kwargs):
        pass

    def _filter_limit(self, *args, **kwargs):
        pass

    def _filter_matches(self, *args, **kwargs):
        pass

    def _filter_sort(self, *args, **kwargs):
        pass

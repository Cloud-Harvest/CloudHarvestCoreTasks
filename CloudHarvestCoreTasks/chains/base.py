from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseTask, TaskStatusCodes
from CloudHarvestCoreTasks.exceptions import TaskTerminationError

from datetime import datetime, timezone
from typing import List, Dict, Any, Generator
from logging import getLogger

from CloudHarvestCoreTasks.exceptions import TaskChainError

logger = getLogger('harvest')


@register_definition(name='chain', category='chain')
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
                 chain_type: str = None,
                 parent: str = None,
                 variables: dict = None,
                 filters: dict = None,
                 *args, **kwargs):
        """
        Initializes a new instance of the BaseTaskChain class.

        Args:
            template(dict): The configuration for the task chain.
                name(str): The name of the task chain.
                tasks(List[dict]): A list of task configurations for the tasks in the chain.
                chain_type(str): The type of the task chain (e.g., 'report', 'harvest').
                description(str, optional): A brief description of what the task chain does. Defaults to None.
                max_workers(int, optional): The maximum number of concurrent workers that are permitted.
            parent (str, optional): A parent request uuid to associate with the task chain. Defaults to None.
            variables(dict, optional): Variables that can be used by the tasks in the chain. The dictionary is merged
                                        with into the BaseTaskChain.variables attribute. Defaults to None.
            filters(dict, optional): A dictionary of user filters to apply to the data. Defaults to an empty dictionary.
        """
        self.original_template = template

        super().__init__()

        from uuid import uuid4
        self.id = kwargs.get('id') or str(uuid4())

        self.name = template['name']
        self.parent = parent
        self.chain_type = chain_type
        self.description = template.get('description')
        self.filters = filters or {}

        # Variables are stored with their name as the key.
        # Starting variables can be added using the `variables` parameter.
        self.variables: Dict[str, Any] = {} | (variables or {})

        self.task_templates: dict or List[dict or BaseTask] = template.get('tasks', [])

        self.status = TaskStatusCodes.initialized
        self.pool = BaseTaskPool(chain=self,
                                 max_workers=template.get('max_workers', 4),
                                 idle_refresh_rate=template.get('idle_refresh_rate', 3),
                                 worker_refresh_rate=template.get('worker_refresh_rate', .5)).start()

        self.position = 0

        self.agent = None       # populated by agent
        self.errors = None
        self.start = None
        self.end = None

        self.meta = {}

        self.required_variables = template.get('required_variables') or []
        self.update_status_client = None

        try:
            # Set up the client used to update the task chain status in Redis
            from CloudHarvestCoreTasks.silos import get_silo
            from redis import StrictRedis
            silo = get_silo('harvest-tasks')

            if silo:
                self.update_status_client: StrictRedis = silo.connect()
                self.update_status()

        except BaseException as ex:
            raise TaskChainError(f'{self.redis_name} failed to connect to `harvest-tasks` silo', ex) from ex

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

        - 'data': Contains a list of dictionaries, each representing a task in the task chain. Each dictionary
          includes the following keys:
            - 'Position': The position of the task in the task chain.
            - 'Name': The name of the task.
            - 'Status': The status of the task.
            - 'Attempts': The number of attempts made to run the task.
            - 'DataBytes': The size of the data produced by the task, in bytes.
            - 'Records': The number of records in the task's data, if applicable.
            - 'Duration': The duration of the task, in seconds.
            - 'Start': The start time of the task.
            - 'End': The end time of the task.

        - 'meta': Contains metadata about the task chain, including the headers for the task metrics.

        Returns:
            dict: A dictionary representing the performance metrics of the task chain.
        """

        if len(self) > 0:
            from sys import getsizeof

            # This part of the report returns results for each task in the task chain.
            task_metrics = [
                {
                    'Position': self.index(task),
                    'Name': task.name,
                    'Class': task.__name__,
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
            starts = min([dt for dt in starts if dt]) if any(starts) else None
            ends = max([dt for dt in ends if dt]) if any(ends) else None

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
            # task_metrics.insert(-1, {k: '' for k in task_metrics[-1].keys()})

        else:
            # If there are no tasks in the task chain, we return a single row with the total metrics.
            task_metrics = [{
                'Position': 'Total',
                'Name': '',
                'Status': self.status.__str__(),
                'Records': 0,
                'DataBytes': 0,
                'Duration': (self.end - self.start).total_seconds() if self.start and self.end else 0,
                'Start': self.start,
                'End': self.end,
            }]

        return task_metrics

    @property
    def redis_name(self) -> str:
        """
        Returns the unique record identifier for the task.
        """

        return f'task:{self.parent or ""}:{self.id}'

    def redis_struct(self) -> dict:
        """
        Returns specific keys stored in Redis.
        """

        return {
            'redis_name': self.redis_name,
            'id': self.id,
            'parent': self.parent,
            'name': self.name,
            'type': self.chain_type,
            'status': self.status,
            'agent': self.agent,
            'position': self.position,
            'total': self.total,
            'start': self.start,
            'end': self.end,
        }

    @property
    def result(self) -> dict:
        """
        Returns the result of the task chain.
        """

        try:
            data = self.variables.get('result') or self[-1].result

        except IndexError:
            data = []

        result = {
            'data': data,
            'errors': self.errors,
            'meta': self.meta,
            'metrics': self.performance_metrics,
            'template': self.original_template
        }

        return result

    @property
    def total(self) -> int:
        """
        Returns the total number of tasks in the task chain.
        """

        return len(self.task_templates)

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

        # Set the possible status codes based on the TaskStatusCodes Enum
        count_result = {
            str(k): 0 for k in TaskStatusCodes.get_codes()
        }

        # Now we count the number of tasks in each status
        for task in self:
            count_result[task.status] += 1

        return {
            'total': self.total,
            'current': self.position,
            'percent': (self.position / self.total) * 100,
            'duration': (self.end or datetime.now(tz=timezone.utc) - self.start).total_seconds() if self.start else 0,
            'counts': count_result
        }

    def find_task_by_name(self, task_name: str) -> 'BaseTask' or None:
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

        return None

    def find_task_position_by_name(self, task_name: str) -> int:
        """
        This method finds the position of a task in the task chain by its name.

        Args:
            task_name (str): The name of the task to find.

        Returns:
            int: The position of the task in the task chain. If the task is not found, it returns None.
        """

        for position, task in enumerate(self.task_templates):
            if task.name == task_name:
                return position

        return 0

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
            raise TaskChainError(self, 'Cannot insert a task before the current task.')

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

        self.results_to_silo()
        self.update_status()

        return self

    def on_error(self, ex: BaseException) -> 'BaseTaskChain':
        """
        Method to run when the task chain errors.
        This method may be overridden in subclasses to provide specific error handling logic.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        self.status = TaskStatusCodes.error

        if self.pool.queue_size:
            self.pool.terminate()

        logger.error(f'{self.redis_name}: Error running task chain {self.name}: {ex}')

        self.results_to_silo()
        self.update_status()

        return self

    def on_start(self) -> 'BaseTaskChain':
        """
        Method to run when the task chain starts.
        This method may be overridden in subclasses to provide specific start logic.
        """

        self.status = TaskStatusCodes.running
        self.start = datetime.now(tz=timezone.utc)
        self.update_status()

        return self

    def results_to_silo(self):
        """
        Sends the TaskChain results to a remote silo.
        """

        if self.update_status_client:
            from CloudHarvestCoreTasks.silos import get_silo
            from CloudHarvestCoreTasks.tasks.redis import format_hset

            try:
                self.update_status_client.hset(
                    name=self.redis_name,
                    mapping=format_hset(self.result or {})
                )

                # Sets an expiration to retrieve the results
                self.update_status_client.expire(self.id, 3600)

            except BaseException as ex:
                raise TaskChainError(self, f'Error storing task chain results in silo `harvest-tasks`: {ex}') from ex

            else:
                logger.debug(f'{self.redis_name}: Stored task chain results in silo `harvest-tasks`')

    def run(self) -> 'BaseTaskChain':
        """
        Runs the task chain. This method will block until all tasks in the chain are completed.
        Note that this method may be overwritten in subclasses to provide specific functionality.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        try:
            self.on_start()
            self.position = 0

            # Validate the required variables are present for the task chain
            if self.required_variables:
                for var in self.required_variables:
                    if var not in self.variables:
                        raise Exception(self, f'Missing required variable: {var}')

            while True:
                # Instantiate the task from the task configuration
                try:
                    from CloudHarvestCoreTasks.factories import template_task_configuration
                    task_template = self.task_templates[self.position]
                    task = template_task_configuration(task_configuration=task_template, task_chain=self)

                    self.update_status()

                    if task.iterate is not None:
                        # Determine how results from the itemized processes will be stored. The default behavior is to override the
                        # variable with the same name as the 'result_as' directive; however, itemized tasks may return results of
                        # different types. The 'result_as' directive provides a way to specify how the results should be stored.
                        result_as = task_template[list(task_template.keys())[0]].get('result_as')

                        if result_as:
                            result_as_mode = result_as.get('mode') or 'override' if isinstance(result_as,dict) else 'override'
                            result_as_name = result_as.get('name') if isinstance(result_as, dict) else result_as

                            # Determine how the results should be stored then initialize the variable accordingly. It is
                            # important to set the variable type here because the results are likely expected in subsequent
                            # tasks. To ensure the next tasks complete successfully, we need to set the variable type
                            # to the expected type.
                            match result_as_mode:
                                case 'append' | 'extend':
                                    self.variables[result_as_name] = []

                                case 'merge':
                                    self.variables[result_as_name] = {}

                                # The default behavior is to override the variable
                                case _:
                                    self.variables[result_as_name] = None

                        from copy import deepcopy
                        # Insert the iterated tasks into the task chain's configurations
                        [
                            self.task_templates.insert(self.position + 1, iter_task)
                            for iter_task in self.iterate_task(original_task_configuration=deepcopy(task_template))
                        ]

                        # Add the parent task to the task chain (it will not be executed)
                        self.append(task)

                        # Flag this task as skipped
                        task.status = TaskStatusCodes.skipped
                        task.meta['Info'] = 'Task was skipped because it was an iterated task.'

                        # Increment the position
                        self.position += 1
                        continue

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
                    raise TaskTerminationError('Task chain was instructed to terminate.')

                # Hold within the loop if there are outstanding pool tasks because the async task might have an
                # on_* directive which needs to be added and processed. By waiting here, we ensure that the task chain
                # will not complete until all tasks have been processed.
                if self.pool.queue_size > 0 and len(self.task_templates) == len(self):
                    self.pool.wait_until_complete()

                # Increment the position
                self.position += 1

            if self.pool.queue_size > 0:
                self.pool.wait_until_complete()

        except BaseException as ex:
            self.on_error(ex)

        finally:
            self.on_complete()

            return self

    def iterate_task(self, original_task_configuration: dict) -> Generator[dict, None, None]:
        """
        This generator converts a task_configuration with an 'iterate' directive into a list of task configurations
        based on the elements of 'iterate.variable'.

        Args:
            original_task_configuration (dict): The original task configuration with the 'iterate' directive.
        """

        # Template the original configuration to get the iterated items. We take this approach to leverage the templating
        # engine to resolve variables in the iterate directive.
        from CloudHarvestCoreTasks.factories import template_task_configuration
        task = template_task_configuration(task_configuration=original_task_configuration, task_chain=self)
        iter_var = task.iterate

        # We employ reversed() here because we want the order of the tasks to be the same as the order of the iterated
        # items. This is because the list.insert() operation will insert the new task at the specified position and
        # shift the existing tasks down the task order. If we iterate in the normal order, the tasks will be performed
        # in the reverse order of the iterated items.
        # iter_var = list(reversed(iter_var))
        for item in reversed(iter_var):
            # Create a deep copy of the original task configuration to avoid mangling the original configuration
            from copy import deepcopy
            task_configuration = deepcopy(original_task_configuration)

            class_key = list(task_configuration.keys())[0]

            # Remove iterable configuration from the task
            task_configuration[class_key].pop('iterate')

            # Update the task's name
            task_configuration[class_key]['name'] = f'{task_configuration[class_key]["name"]} - {iter_var.index(item) + 1}/{len(iter_var)}'

            # Template the task with the item
            from CloudHarvestCoreTasks.factories import template_task_configuration
            itemized_task_configuration = template_task_configuration(task_configuration,
                                                                      task_chain=self,
                                                                      item=item,
                                                                      instantiate=False)

            yield itemized_task_configuration

    def terminate(self) -> 'BaseTaskChain':
        """
        Terminates the task chain.
        This method may be overridden in subclasses to provide specific termination logic.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        self.status = TaskStatusCodes.terminating
        self.update_status()
        self.pool.terminate()

        return self

    def update_status(self):
        """
        Sends the TaskChain status to Redis.
        """
        from CloudHarvestCoreTasks.tasks.redis import format_hset
        if self.update_status_client:
            try:
                self.update_status_client.hset(name=self.redis_name, mapping=format_hset(self.redis_struct()))
                self.update_status_client.expire(name=self.redis_name, time=3600)

            except BaseException as ex:
                raise TaskChainError(self, f'Error updating task chain status: {ex}') from ex

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

    def __init__(self, chain: BaseTaskChain, max_workers: int, idle_refresh_rate: float = 3,
                 worker_refresh_rate: float = .5):
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

        self._pool = []  # List of tasks waiting to be executed
        self._active = []  # List of tasks currently being executed
        self._complete = []  # List of tasks that have completed execution

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
                if task.status in (TaskStatusCodes.complete, str(TaskStatusCodes.error), TaskStatusCodes.skipped):
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

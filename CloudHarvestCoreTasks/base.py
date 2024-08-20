from CloudHarvestCorePluginManager.decorators import register_definition
from .exceptions import BaseTaskException
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List
from logging import getLogger


logger = getLogger('harvest')

# TODO: (Async)TemplateTask (a task that generates more tasks from a template) with parameters to insert the new tasks
#       into a specific task chain position (or immediately following itself)


class TaskStatusCodes(Enum):
    """
    These are the basic status codes for any given data collection object.
    """
    complete = 'complete'           # the thread has stopped and there are no more tasks to complete
    error = 'error'                 # the thread has stopped in an error state
    idle = 'idle'                   # the thread is running but has no outstanding tasks
    initialized = 'initialized'     # the thread has been created
    running = 'running'             # the thread is currently processing data
    skipped = 'skipped'             # the thread was skipped and did not run because a `when` condition was not mets
    terminating = 'terminating'     # the thread was ordered to stop and is currently attempting to shut down


class TaskConfiguration:
    """
    The TaskConfiguration class is responsible for managing the configuration of a task.

    It stores the configuration of a task and provides methods to instantiate the task based on this configuration.
    If a task chain is provided, it retrieves the variables from the task chain and uses them to template the task configuration.

    Attributes:
        class_name (str): The name of the task class to instantiate.
        task_configuration (dict): The configuration for the task.
        name (str): The name of the task.
        description (str): A brief description of what the task does.
        task_chain (BaseTaskChain): The task chain that the task belongs to.
        task_class (BaseTask): The class of the task to instantiate.
        instantiated_class (BaseTask): The instantiated task.

    Methods:
        instantiate() -> BaseTask: Instantiates a task based on the task configuration.
    """

    def __init__(self, task_configuration: dict, task_chain: 'BaseTaskChain' = None, **kwargs):
        """
        Initializes a new instance of the TaskConfiguration class.

        Args:
            task_configuration (dict): The configuration for the task.
            task_chain (BaseTaskChain, optional): The task chain that the task belongs to. Defaults to None.
            extra_vars (dict, optional): Extra variables that can be used to template the task configuration. Defaults to None.
        """

        self.class_name = list(task_configuration.keys())[0]
        self.task_configuration = task_configuration[self.class_name].copy()

        self.name = self.task_configuration['name']
        self.description = self.task_configuration.get('description')

        self.task_chain = task_chain

        self.instantiated_class = None
        self.kwargs = kwargs

        # Retrieve the class of the task to instantiate
        from CloudHarvestCorePluginManager import Registry

        try:
            self.task_class = Registry.find_definition(class_name=self.class_name, is_subclass_of=BaseTask)[0]

        except IndexError:
            raise BaseTaskException(f'Could not find a task class named {self.class_name}.')

    def instantiate(self) -> 'BaseTask':
        """
        Instantiates a task based on the task configuration.

        This method uses the task configuration stored in the instance to instantiate a task. If a task chain is
        provided, it retrieves the variables from the task chain and uses them to template the task configuration.
        The templated configuration is then used to instantiate the task.

        Returns:
            BaseTask: The instantiated task.
        """

        from .templating.functions import template_object

        # If a task chain is provided, get its variables. Otherwise, use an empty dictionary.
        task_chain_vars = self.task_chain.get_variables_by_names(self.task_configuration.get('with_vars')) \
            if self.task_chain and self.task_configuration.get('with_vars') else {}

        # Template the task configuration with the variables from the task chain
        templated_class_kwargs = template_object(template=self.task_configuration,
                                                 variables=task_chain_vars)

        # Instantiate the task with the templated configuration and return it
        self.instantiated_class = self.task_class(task_chain=self.task_chain, **templated_class_kwargs | self.kwargs)

        return self.instantiated_class


class BaseTask:
    def __init__(self,
                 name: str,
                 blocking: bool = True,
                 description: str = None,
                 on: dict = None,
                 task_chain: 'BaseTaskChain' = None,
                 result_as: str = None,
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
            blocking (bool): A boolean indicating whether the task is blocking or not. If True, the task will block until it completes.
            description (str): A brief description of what the task does.
            on (dict): A dictionary of task configurations for the task to run when it completes, errors, is skipped, or starts.
            task_chain (BaseTaskChain): The task chain that this task belongs to, if applicable.
            with_vars (list[str]): A list of variables that this task uses. These variables are retrieved from the task chain
                and used in TaskConfiguration which templates this task's class.
            status (TaskStatusCodes): The current status of the task.
            data (Any): The data that this task produces, if applicable.
            meta (Any): Any metadata associated with this task.
            result_as (str): The name of the variable to store the result of this task in the task chain's variables.
            when (str): A string representing a conditional argument using Jinja2 templating. If provided, the task
                will only run if the condition evaluates to True.
        """

        self.name = name
        self.blocking = blocking
        self.description = description
        self.task_chain = task_chain
        self.on = on or {}

        self.with_vars = with_vars
        self.status = TaskStatusCodes.initialized

        self.data = None
        self.meta = None
        self.start = None
        self.end = None
        self.result_as = result_as
        self.when = when

    @property
    def duration(self) -> float:
        """
        Returns the duration of the task in seconds.
        """
        return ((self.end or datetime.now(tz=timezone.utc)) - self.start).total_seconds() if self.start else -1

    def method(self):
        """
        This method should be overwritten in subclasses to provide specific functionality.
        """
        # Example code to simulate a long-running task.
        for i in range(10):

            # Make sure to include a block which handles termination
            if self.status == TaskStatusCodes.terminating:
                from exceptions import TaskTerminationException
                raise TaskTerminationException('Task was instructed to terminate.')

            from time import sleep
            sleep(1)

        # Set the data attribute to the result of the task, otherwise `as_result` will not populate.
        self.data = {'Test': 'Result'}

        return self

    def run(self) -> 'BaseTask':
        """
        Runs the task. This method will block until it completes, errors, or is terminated.

        Returns:
        BaseTask: The instance of the task.
        """

        try:
            try:
                self.on_start()

                when_result = True

                # Check of the `when` condition is met
                if self.when and self.task_chain:
                    from .templating.functions import template_object
                    when_result = True if template_object(template={'result': '{{ ' + self.when + ' }}'}, variables=self.task_chain.variables).get('result') == 'True' else False

                # If `self.when` condition is met or is None, run the method
                if when_result:
                    self.method()

                # Skip the task
                else:
                    self.on_skipped()

            except Exception as ex:
                self.on_error(ex)

            else:
                # If the task was not skipped, call the on_complete() method
                if self.status != TaskStatusCodes.skipped:
                    self.on_complete()

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

        for d in (self.on.get(directive) or []):
            # If the task is blocking, insert the new task before the next task in the chain
            if self.blocking:
                self.task_chain.task_templates.insert(self.task_chain.position + i,
                                                      TaskConfiguration(task_configuration=d,
                                                                        task_chain=self.task_chain))

            # If the task is not blocking, append the new task to the end of the chain since the position of the current
            # task is not known.
            else:
                self.task_chain.task_templates.append(TaskConfiguration(task_configuration=d,
                                                                        task_chain=self.task_chain))

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
            self.task_chain.variables[self.result_as] = self.data

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

    def __dict__(self) -> dict:
        return {
            'name': self.name,
            'description': self.description,
            'status': self.status,
            'data': self.data,
            'meta': self.meta
        }


class BaseAuthenticationTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.auth = None


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
        insert_task_after_name(task_name: str, new_task_configuration: TaskConfiguration) -> 'BaseTaskChain': Inserts a new task into the task chain immediately after a task with a given name.
        insert_task_before_name(task_name: str, new_task_configuration: TaskConfiguration) -> 'BaseTaskChain': Inserts a new task into the task chain immediately before a task with a given name.
        insert_task_at_position(position: int, new_task_configuration: TaskConfiguration) -> 'BaseTaskChain': Inserts a new task into the task chain at a specific position.
        on_complete() -> 'BaseTaskChain': Method to run when the task chain completes.
        on_error(ex: Exception) -> 'BaseTaskChain': Method to run when the task chain errors.
        run() -> 'BaseTaskChain': Runs the task chain.
        terminate() -> 'BaseTaskChain': Terminates the task chain.
    """

    def __init__(self,
                 template: dict,
                 extra_vars: dict = None,
                 *args, **kwargs):
        """
        Initializes a new instance of the BaseTaskChain class.

        Args:
            template(dict): The configuration for the task chain.
                name(str): The name of the task chain.
                tasks(list[dict]): A list of task configurations for the tasks in the chain.
                description(str, optional): A brief description of what the task chain does. Defaults to None.
                max_workers(int, optional): The maximum number of concurrent workers that are permitted.
        """
        super().__init__()

        self.name = template['name']
        self.description = template.get('description')

        self.variables = {}
        self.task_templates: List[TaskConfiguration] = [
            TaskConfiguration(task_configuration=t,
                              task_chain=self,
                              extra_vars=extra_vars,
                              **kwargs)
            for t in template.get('tasks', [])
        ]

        self.status = TaskStatusCodes.initialized
        self.pool = BaseTaskPool(chain=self,
                                 max_workers=template.get('max_workers', 4),
                                 idle_refresh_rate=template.get('idle_refresh_rate', 3),
                                 worker_refresh_rate=template.get('worker_refresh_rate', .5)).start()

        self.position = 0

        self.start = None
        self.end = None

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
        from statistics import mean, stdev

        # This part of the report returns results for each task in the task chain.
        task_metrics = [
            {
                'Position': self.position,
                'Name': task.name,
                'Status': task.status.__str__(),
                'DataBytes': getsizeof(task.data),
                'Records': len(task.data) if hasattr(task.data, '__len__') else 'N/A',
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
            if hasattr(task.data, '__len__'):
                total_records.append(len(task.data))

            total_result_size.append(getsizeof(task.data))
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
                    'data': self._data or self.data or self[-1].data,
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
            if task.name == task_name:
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

    def insert_task_after_name(self, task_name: str, new_task_configuration: TaskConfiguration) -> 'BaseTaskChain':
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

    def insert_task_before_name(self, task_name: str, new_task_configuration: TaskConfiguration) -> 'BaseTaskChain':
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

    def insert_task_at_position(self, position: int, new_task_configuration: TaskConfiguration) -> 'BaseTaskChain':
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

        try:
            self.on_start()
            self.position = 0

            while True:
                # Instantiate the task from the task configuration
                task = self.task_templates[self.position].instantiate()
                self.append(task)

                # Execute the task
                if task.blocking:
                    task.run()

                else:
                    self.pool.add(task)

                # Escape after completing the last task
                if self.position == self.total:
                    break

                # Check for termination
                if self.status == TaskStatusCodes.terminating:
                    from .exceptions import TaskTerminationException
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

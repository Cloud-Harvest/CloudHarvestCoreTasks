from .exceptions import BaseTaskException
from enum import Enum
from typing import List, Literal
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
    initialized = 'initialized'     # the thread has been created
    running = 'running'             # the thread is currently processing data
    terminating = 'terminating'     # the thread was ordered to stop and is currently attempting to shut down


class TaskRegistry:
    """
    This class is a registry for all Tasks available in the program. Tasks are automatically added when they inherit
    BaseTask. As a result, all tasks should inherit BaseTask even if they do not use the BaseTask functionality.
    """
    tasks = []

    @staticmethod
    def add_subclass(subclass: 'BaseTask' or 'BaseTaskChain'):
        """
        Add a subclass to the TaskRegistry.
        Args:
            subclass: The subclass to add.
        """
        import re

        if issubclass(subclass, BaseTask):
            class_name = subclass.__name__[0:-4]

        elif issubclass(subclass, BaseTaskChain):
            class_name = subclass.__name__[0:-9]

        else:
            raise TypeError(f'Cannot add subclass of type {type(subclass)} to TaskRegistry. '
                            f'Must be subclass of BaseTask or BaseTaskChain.')

        class_name = str(class_name[0] + re.sub(r'([A-Z])', r'_\1', class_name[1:])).lower()

        if class_name not in TaskRegistry.tasks:
            TaskRegistry.tasks.append({class_name: subclass})

    @staticmethod
    def get_task_class_by_name(target_name: str,
                               target_task_type: Literal['task', 'taskchain']) -> 'BaseTask' or 'BaseTaskChain' or None:
        """
        This method retrieves a task class by its name and type.
        Args:
            target_name: The desired task name.
            target_task_type: The desired task type.

        Returns:
            'BaseTask', 'BaseTaskChain', or None
        """

        for task in TaskRegistry.tasks:
            for task_name, task_type in task.items():
                if task_name == target_name:
                    match target_task_type:
                        case 'task':
                            if isinstance(task_type, BaseTask) or issubclass(task_type, BaseTask):
                                return task_type

                        case 'taskchain':
                            if isinstance(task_type, BaseTaskChain) or issubclass(task_type, BaseTaskChain):
                                return task_type


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
        extra_vars (dict): Extra variables that can be used to template the task configuration.
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

        for class_name, task_configuration in task_configuration.items():
            self.class_name = class_name
            self.task_configuration = task_configuration.copy()
            break

        self.name = self.task_configuration['name']
        self.description = self.task_configuration.get('description')

        self.task_chain = task_chain
        self.task_class = TaskRegistry.get_task_class_by_name(self.class_name, target_task_type='task')

        self.instantiated_class = None
        self.kwargs = kwargs

    def instantiate(self) -> 'BaseTask':
        """
        Instantiates a task based on the task configuration.

        This method uses the task configuration stored in the instance to instantiate a task. If a task chain is
        provided, it retrieves the variables from the task chain and uses them to template the task configuration.
        The templated configuration is then used to instantiate the task.

        Returns:
            BaseTask: The instantiated task.
        """

        from templating.functions import template_object

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
                 description: str = None,
                 task_chain: 'BaseTaskChain' = None,
                 result_as: str = None,
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
            description (str): A brief description of what the task does.
            task_chain (BaseTaskChain): The task chain that this task belongs to, if applicable.
            with_vars (list[str]): A list of variables that this task uses.
            status (TaskStatusCodes): The current status of the task.
            data (Any): The data that this task produces, if applicable.
            meta (Any): Any metadata associated with this task.
            result_as (str): The name of the variable to store the result of this task in the task chain's variables.
        """

        self.name = name
        self.description = description
        self.task_chain = task_chain

        self.with_vars = with_vars
        self.status = TaskStatusCodes.initialized

        self.data = None
        self.meta = None
        self.result_as = result_as

    def run(self) -> 'BaseTask':
        """
        Runs the task. This method will block until all conditions specified in the constructor are met.
        Note that this method should be overwritten in subclasses to provide specific functionality.

        Recommendations:
        - Include on_complete() and on_error() calls within custom run() methods.
        - Capture when the task_chain.status is set to 'terminating' and exit the run() method if prudent.
        - Return self at the end of the run() method.

        Returns:
            BaseTask: The instance of the task.
        """

        return self

    def on_complete(self) -> 'BaseTask':
        """
        Method to run when a task completes.
        This method may be overridden in subclasses to provide specific completion logic.

        Returns:
            BaseTask: The instance of the task.
        """

        if self.result_as and self.task_chain:
            self.task_chain.variables[self.result_as] = self.data

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
            self.meta = ex.args

        logger.error(f'Error running task {self.name}: {ex}')

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

    def __init_subclass__(cls, **kwargs):
        """
        This method is called when a subclass of BaseTask is created.
        """
        super().__init_subclass__(**kwargs)
        TaskRegistry.add_subclass(cls)

    def __dict__(self) -> dict:
        return {
            'name': self.name,
            'description': self.description,
            'status': self.status,
            'data': self.data,
            'meta': self.meta
        }


class BaseAsyncTask(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.thread = None

    def run(self, *args, **kwargs) -> 'BaseAsyncTask':
        """
        Override this method with code to run a task asynchronously.
        """
        from threading import Thread

        self.thread = Thread(target=self._run, args=args, kwargs=kwargs)
        self.thread.start()

        self.status = TaskStatusCodes.running

        return self

    def _run(self, *args, **kwargs):
        """
        Override this method with code to run a task.
        """
        pass

    def terminate(self) -> 'BaseAsyncTask':
        super().terminate()
        self.thread.join()

        return self


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
        meta (Any): Any metadata associated with the task chain.

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

    def __init__(self, template: dict, extra_vars: dict = None, *args, **kwargs):
        """
        Initializes a new instance of the BaseTaskChain class.

        Args:
            template(dict): The configuration for the task chain.
                name(str): The name of the task chain.
                tasks(list[dict]): A list of task configurations for the tasks in the chain.
                description(str, optional): A brief description of what the task chain does. Defaults to None.
        """

        # TODO: Check that any AsyncTask (except PruneTask) have a WaitTask at some point after it.
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
    def result(self) -> dict:
        """
        Returns the result of the task chain. This can be interpreted either as the 'result' variable in the task
        chain's variables or the data and meta of the last task in the chain.
        """
        return {
            'data': self._data or self.data or self[-1].data,
            'meta': self._meta or self[-1].meta
        }

    @property
    def total(self) -> int:
        """
        Returns the total number of tasks in the task chain.
        """
        return len(self.task_templates)

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
        logger.error(f'Error running task chain {self.name}: {ex}')

        return self

    def run(self) -> 'BaseTaskChain':
        """
        Runs the task chain. This method will block until all tasks in the chain are completed.
        Note that this method may be overwritten in subclasses to provide specific functionality.

        Returns:
            BaseTaskChain: The instance of the task chain.
        """

        from datetime import datetime, timezone
        self.status = TaskStatusCodes.running

        self.start = datetime.now(tz=timezone.utc)

        try:
            self.position = 0

            while True:
                # Instantiate the task from the task configuration
                task = self.task_templates[self.position].instantiate()
                self.append(task)

                # Execute the task
                task.run()

                # Increment the position
                self.position += 1

                # Escape after completing the last task
                if self.position == self.total:
                    break

                if self.status == TaskStatusCodes.terminating:
                    from .exceptions import TaskTerminationError
                    raise TaskTerminationError('Task chain was instructed to terminate.')

        except Exception as ex:
            self.on_error(ex)

        finally:
            self.end = datetime.now(tz=timezone.utc)
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

    def __init_subclass__(cls, **kwargs):
        """
        This method is called when a subclass of BaseTask is created.
        """
        super().__init_subclass__(**kwargs)
        TaskRegistry.add_subclass(cls)

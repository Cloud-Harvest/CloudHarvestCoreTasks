from CloudHarvestCorePluginManager.decorators import register_definition
from typing import List, Literal
from .data_model.recordset import HarvestRecordSet
from .base import BaseTask, BaseTaskChain, TaskStatusCodes


@register_definition(name='delay')
class DelayTask(BaseTask):
    """
    The DelayTask class is a subclass of the BaseTask class. It represents a task that introduces a delay in the task
    chain execution.

    Attributes:
        delay_seconds (float): The duration of the delay in seconds.

    Methods:
        method(): Overrides the run method of the BaseTask class. It introduces a delay in the task chain execution.

    Example:
        delay_task = DelayTask(delay_seconds=5)
        delay_task.run()        # This will introduce a delay of 5 seconds.
    """

    def __init__(self, delay_seconds: float = None, **kwargs):
        """
        Initializes a new instance of the DelayTask class.

        Args:
            delay_seconds (float): The duration of the delay in seconds.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(**kwargs)
        self.delay_seconds = delay_seconds

    def method(self) -> 'DelayTask':
        """
        This method will introduce a delay in the task chain execution.

        The delay is introduced using the sleep function from the time module.
        The duration of the delay is specified by the delay_seconds attribute.

        The method also checks the status of the task during the delay. If the status changes to 'terminating',
        the delay is interrupted and the method exits.

        Once the delay is over or interrupted, the on_complete method is called to mark the task as complete or
        terminating respectively.

        Example:
            delay_task = DelayTask(delay_seconds=5)
            delay_task.run()  # This will introduce a delay of 5 seconds or less if the task is terminated earlier.
        """
        from datetime import datetime, timezone
        from time import sleep

        while (datetime.now(tz=timezone.utc) - self.start).total_seconds() < self.delay_seconds:
            sleep(1)

            if self.status == TaskStatusCodes.terminating:
                break

        return self


@register_definition(name='dummy')
class DummyTask(BaseTask):
    """
    The DummyTask class is a subclass of the Base
    Task class. It represents a task that does nothing when run. Used for testing.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes a new instance of the DummyTask class.
        """
        super().__init__(*args, **kwargs)

    def method(self) -> 'DummyTask':
        """
        This method does nothing. It is used to represent a task that does nothing when run.

        Returns:
            DummyTask: The current instance of the DummyTask class.
        """
        self.data = [{'dummy': 'data'}]
        self.meta = {'info': 'this is dummy metadata'}

        return self


@register_definition(name='error')
class ErrorTask(BaseTask):
    """
    The ErrorTask class is a subclass of the BaseTask class. It represents a task that raises an exception when run.
    This task is used for testing error handling in task chains and should not be used in production code. For example,
    this task is used for testing the `on: error` directive in task chain configurations.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def method(self):
        raise Exception('This is an error task')


@register_definition(name='file')
class FileTask(BaseTask):
    """
    The FileTask class is a subclass of the BaseTask class. It represents a task that performs file operations such as
    reading from or writing to a file. Read operations take the content of a file and places them in the TaskChain's
    variables. Write operations take the data from the TaskChain's variables and write them to a file.

    Methods:
        determine_format(): Determines the format of the file based on its extension.
        method(): Performs the file operation specified by the mode and format attributes.
    """

    def __init__(self,
                 path: str,
                 result_as: str,
                 mode: Literal['append', 'read', 'write'],
                 desired_keys: List[str] = None,
                 format: Literal['config', 'csv', 'json', 'raw', 'yaml'] = None,
                 template: str = None,
                 *args, **kwargs):

        """
        Initializes a new instance of the FileTask class.

        Args:
            path (str): The path to the file.
            result_as (str): The name under which the result will be stored.
            mode (Literal['append', 'read', 'write']): The mode in which the file will be opened.
            desired_keys (List[str], optional): A list of keys to filter the data by. Defaults to None.
            format (Literal['config', 'csv', 'json', 'raw', 'yaml'], optional): The format of the file. Defaults to None.
            template (str, optional): A template to use for the output. Defaults to None.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """

        super().__init__(*args, **kwargs)
        from pathlib import Path

        # Class-specific
        self.path = path
        self.abs_path = Path(self.path).expanduser().absolute()
        self.mode = mode.lower()
        self.format = format.lower() or self.determine_format()
        self.desired_keys = desired_keys
        self.template = template

        # Overrides from BaseTask
        self.result_as = result_as

    def determine_format(self):
        """
        Determines the format of the file based on its extension.

        Returns:
            str: The format of the file.
        """

        supported_extensions_and_formats = {
            'cfg': 'config',
            'conf': 'config',
            'config': 'config',
            'csv': 'csv',
            'ini': 'config',
            'json': 'json',
            'yaml': 'yaml',
            'yml': 'yaml'
        }

        path_file_extensions = str(self.path.split('.')[-1]).lower()

        return supported_extensions_and_formats.get(path_file_extensions) or 'raw'

    def method(self):
        """
        Performs the file operation specified by the mode and format attributes.

        This method handles both reading from and writing to files in various formats such as config, csv, json, raw, and yaml.

        Raises:
            BaseTaskException: If the data format is not supported for the specified operation.

        Returns:
            self: Returns the instance of the FileTask.
        """

        from .exceptions import BaseTaskException

        from configparser import ConfigParser
        from csv import DictReader, DictWriter
        import json
        import yaml

        modes = {
            'append': 'a',
            'read': 'r',
            'write': 'w'
        }

        # Open the file in the specified mode
        with open(self.abs_path, modes[self.mode]) as file:
            
            # Read operations
            if self.mode == 'read':
                if self.format == 'config':
                    config = ConfigParser()
                    config.read_file(file)
                    result = {section: dict(config[section]) for section in config.sections()}

                elif self.format == 'csv':
                    result = list(DictReader(file))

                elif self.format == 'json':
                    result = json.load(file)

                elif self.format == 'yaml':
                    result = yaml.load(file, Loader=yaml.FullLoader)

                else:
                    result = file.read()

                # If desired_keys is specified, filter the result to just those keys
                if self.desired_keys:
                    if isinstance(result, dict):
                        self.data = {k: v for k, v in result.items() if k in self.desired_keys}
                    
                    elif isinstance(result, list):
                        self.data = [{k: v for k, v in record.items() if k in self.desired_keys} for record in result]
                
                # Return the entire result
                else:
                    self.data = result

            # Write operations
            else:
                # Retrieve the data to write to the file
                _data = self.task_chain.get_variables_by_names(*self.with_vars) or {}

                # If with_vars is a single variable, use that as the data
                _data = _data.get(self.with_vars[0]) if len(self.with_vars) == 1 else _data

                # If the user has provided a template for the output, apply it
                if self.template:
                    from templating.functions import template_object
                    _data = template_object(template=self.template, variables=_data)

                # If no template is provided, use _data as provided
                else:
                    # If the user has specified desired_keys, filter the data to just those keys, if applicable
                    if self.desired_keys:
                        from flatten_json import flatten, unflatten
                        separator = '.'

                        # If the data is a dictionary, flatten it, filter the keys, and unflatten it
                        if isinstance(_data, dict):
                            _data = unflatten({
                                k: v for k, v in flatten(_data.items, separator=separator)()
                                if k in self.desired_keys
                            }, separator=separator)

                        # If the data is a list, flatten each record, filter the keys, and unflatten each record
                        elif isinstance(_data, list):
                            _data = [
                                unflatten({
                                    k: v for k, v in flatten(record, separator=separator).items()
                                    if k in self.desired_keys
                                })
                                for record in _data
                            ]

                if self.format == 'config':
                    config = ConfigParser()
                    if isinstance(_data, dict):
                        config.read_dict(_data)

                        config.write(file)

                    else:
                        raise BaseTaskException(f'{self.name}: `FileTask` only supports dictionaries for writes to config files.')

                elif self.format == 'csv':
                    if isinstance(_data, list):
                        if all([isinstance(record, dict) for record in _data]):
                            consolidated_keys = set([key for record in _data for key in record.keys()])
                            use_keys = self.desired_keys or consolidated_keys

                            writer = DictWriter(file, fieldnames=use_keys)
                            writer.writeheader()

                            writer.writerows(_data)

                            return self

                    raise BaseTaskException(f'{self.name}: `FileTask` only supports lists of dictionaries for writes to CSV files.')

                elif self.format == 'json':
                    json.dump(_data, file, default=str, indent=4)

                elif self.format == 'yaml':
                    yaml.dump(_data, file)

                else:
                    file.write(str(_data))

        return self


@register_definition(name='recordset')
class HarvestRecordSetTask(BaseTask):
    """
    The HarvestRecordSetTask class is a subclass of the BaseTask class. It represents a task that operates on a record set.

    Attributes:
        recordset_name (HarvestRecordSet): The name of the record set this task operates on.
        stages: A list of dictionaries containing the function name and arguments to be applied to the recordset.
        >>> stages = [
        >>>     {
        >>>         'function_name': {
        >>>             'argument1': 'value1',
        >>>             'argument2': 'value2'
        >>>         }
        >>>     }
        >>> ]

    Methods:
        method(): Executes the function on the record set with the provided arguments and stores the result in the data attribute.
    """

    def __init__(self, recordset_name: HarvestRecordSet, stages: List[dict], *args, **kwargs):
        """
        Constructs a new HarvestRecordSetTask instance.

        Args:
            recordset_name (HarvestRecordSet): The name of the record set this task operates on.
            stages (List[dict]): A list of dictionaries containing the function name and arguments to be applied to the recordset.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        self.recordset_name = recordset_name
        self.stages = stages
        self.position = 0

    def method(self):
        """
        Executes functions on the recordset with the provided function and arguments, then stores the result in the data attribute.

        This method iterates over the `stages` defined for this task. For each stage, it retrieves the function and its arguments.
        It then checks if the function is a method of the HarvestRecordSet or HarvestRecord class. If it is, it applies the function to the record set or each record in the record set, respectively.
        If the function is not a method of either class, it raises an AttributeError.

        The result of applying the function is stored in the data attribute of the HarvestRecordSetTask instance.

        Returns:
            self: Returns the instance of the HarvestRecordSetTask.
        """

        from .data_model.record import HarvestRecord
        from .data_model.recordset import HarvestRecordSet

        # Get the recordset from the task chain variables
        recordset = self.task_chain.get_variables_by_names(self.recordset_name).get(self.recordset_name)

        for stage in self.stages:
            # Record the position of stages completed
            self.position += 1

            # Each dictionary should only contain one key-value pair
            for function, arguments in stage.items():

                # This is a HarvestRecordSet command
                if hasattr(HarvestRecordSet, function):
                    getattr(recordset, function)(**arguments or {})

                # This is a HarvestRecord command
                elif hasattr(HarvestRecord, function):
                    [
                        getattr(record, function)(**arguments or {})
                        for record in recordset
                    ]

                else:
                    raise AttributeError(f"Neither HarvestRecordSet nor HarvestRecord has a method named '{function}'")

                break

        self.data = recordset

        return self


@register_definition(name='prune')
class PruneTask(BaseTask):
    def __init__(self, previous_task_data: bool = False, stored_variables: bool = False, *args, **kwargs):
        """
        Prunes the task chain.

        This method can be used to clear the data of previous tasks and/or the stored variables in the task chain.
        This can be useful to free up memory during the execution of a long task chain.

        Args:
            previous_task_data (bool, optional): If True, the data of all previous tasks in the task chain will be cleared. Defaults to False.
            stored_variables (bool, optional): If True, all variables stored in the task chain will be cleared. Defaults to False.

        Returns:
            BaseTaskChain: The current instance of the task chain.
        """

        super().__init__(*args, **kwargs)
        self.previous_task_data = previous_task_data
        self.stored_variables = stored_variables

    def method(self) -> 'PruneTask':
        from sys import getsizeof
        total_bytes_pruned = 0

        # If previous_task_data is True, clear the data of all previous tasks
        if self.previous_task_data:
            for i in range(self.task_chain.position):
                if hasattr(self.task_chain[i], 'data'):
                    total_bytes_pruned += getsizeof(self.task_chain[i].data)
                    setattr(self.task_chain[i], 'data', None)

        # If stored_variables is True, clear all variables stored in the task chain
        if self.stored_variables:
            total_bytes_pruned += getsizeof(self.task_chain.variables)
            self.task_chain.variables.clear()

        self.data = {
            'total_bytes_pruned': total_bytes_pruned
        }

        return self


@register_definition(name='template')
class TemplateTask(BaseTask):
    def __init__(self, template: dict,
                 records: (List[dict] or str) = None,
                 insert_tasks_at_position: int = None,
                 insert_tasks_before_name: str = None,
                 insert_tasks_after_name: str = None,
                 **kwargs):

        super().__init__(**kwargs)

        self.template = template
        self.records = records if isinstance(records, list) else self.task_chain.variables.get(records)

        # Insert position for tasks
        self.insert_tasks_at_position = insert_tasks_at_position
        self.insert_tasks_before_name = insert_tasks_before_name
        self.insert_tasks_after_name = insert_tasks_after_name

    def method(self, *args, **kwargs) -> 'TemplateTask':
        for record in self.records:
            from .base import TaskConfiguration
            task_configuration = TaskConfiguration(task_configuration=self.template.copy(),
                                                   task_chain=self.task_chain,
                                                   extra_vars=record)

            if self.insert_tasks_at_position:
                self.task_chain.task_templates.insert(self.insert_tasks_at_position, task_configuration)

            elif self.insert_tasks_before_name:
                self.task_chain.insert_task_before_name(self.insert_tasks_before_name, task_configuration)

            elif self.insert_tasks_after_name:
                self.task_chain.insert_task_after_name(self.insert_tasks_after_name, task_configuration)

            else:
                self.task_chain.task_templates.append(task_configuration)

        return self


@register_definition(name='wait')
class WaitTask(BaseTask):
    def __init__(self,
                 chain: BaseTaskChain,
                 position: int,
                 check_time_seconds: float = 1,
                 when_all_previous_async_tasks_complete: bool = False,
                 when_all_previous_tasks_complete: bool = False,
                 when_all_tasks_by_name_complete: List[str] = None,
                 when_any_tasks_by_name_complete: List[str] = None,
                 **kwargs):

        """
        The WaitTask class is a subclass of the BaseTask class. It represents a task that waits for certain conditions
        to be met before proceeding. It is most useful in task chains where asynchronous tasks are used.

        Initializes a new instance of the WaitTask class.

        Args:
            chain (BaseTaskChain): The task chain that this task belongs to.
            position (int): The position of this task in the task chain.
            check_time_seconds (float, optional): The time interval in seconds at which this task checks if its conditions are met. Defaults to 1.
            when_all_previous_async_tasks_complete (bool, optional): A flag indicating whether this task should wait for all previous async tasks to complete. Defaults to False.
            when_all_previous_tasks_complete (bool, optional): A flag indicating whether this task should wait for all previous tasks to complete. Defaults to False.
            when_all_tasks_by_name_complete (List[str], optional): A list of task names. This task will wait until all tasks with these names are complete. Defaults to None.
            when_any_tasks_by_name_complete (List[str], optional): A list of task names. This task will wait until any task with these names is complete. Defaults to None.
        """

        self.chain = chain
        self.position = position
        self.check_time_seconds = check_time_seconds
        self._when_all_previous_async_tasks_complete = when_all_previous_async_tasks_complete
        self._when_all_previous_tasks_complete = when_all_previous_tasks_complete
        self._when_all_tasks_by_name_complete = when_all_tasks_by_name_complete
        self._when_any_tasks_by_name_complete = when_any_tasks_by_name_complete

        super().__init__(**kwargs)

    def method(self, *args, **kwargs):
        """
        Runs the task. This method will block until the conditions specified by the task attributes are met.
        """
        from time import sleep

        while True:
            if any([
                self.when_all_previous_async_tasks_complete,
                self.when_all_previous_tasks_complete,
                self.when_all_tasks_by_name_complete,
                self.when_any_tasks_by_name_complete,
                self.status == TaskStatusCodes.terminating
            ]):
                break

            sleep(self.check_time_seconds)

    @property
    def when_all_previous_async_tasks_complete(self) -> bool:
        """
        Checks if all previous async tasks are complete.

        Returns:
            bool: True if all previous AsyncTasks are complete, False otherwise.
        """

        if self._when_all_previous_async_tasks_complete:
            return all([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.chain[0:self.position]
                if task.blocking is False
            ])

    @property
    def when_all_previous_tasks_complete(self) -> bool:
        """
        Checks if all previous tasks are complete.

        Returns:
            bool: True if all previous tasks are complete, False otherwise.
        """

        if self._when_all_previous_tasks_complete:
            return all([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.chain[0:self.position]
            ])

    @property
    def when_all_tasks_by_name_complete(self) -> bool:
        """
        Checks if all tasks with the specified names are complete.

        Returns:
            bool: True if all tasks with the specified names are complete, False otherwise.
        """

        if self._when_all_tasks_by_name_complete:
            return all([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.chain[0:self.position]
                if task.name in self._when_all_tasks_by_name_complete
            ])

    @property
    def when_any_tasks_by_name_complete(self) -> bool:
        """
        Checks if any task with the specified names is complete.

        Returns:
            bool: True if any task with the specified names is complete, False otherwise.
        """

        if self._when_any_tasks_by_name_complete:
            return any([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.chain[0:self.position]
                if task.name in self._when_all_tasks_by_name_complete
            ])

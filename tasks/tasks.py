from typing import Any, List, Iterable

from .base import BaseAsyncTask, BaseTask, BaseTaskChain, TaskStatusCodes


# class ForEachTask(BaseTask):
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#
#     def run(self, function: Any, *args, **kwargs):
#         for task in self.chain[self.position:]:
#             if task.status == TaskStatusCodes.complete:
#                 continue
#
#             function(task, *args, **kwargs)
#
#     def on_complete(self):
#         self.status = TaskStatusCodes.complete


class DelayTask(BaseTask):
    """
    The DelayTask class is a subclass of the BaseTask class. It represents a task that introduces a delay in the task
    chain execution.

    Attributes:
        delay_seconds (float): The duration of the delay in seconds.

    Methods:
        run(): Overrides the run method of the BaseTask class. It introduces a delay in the task chain execution.

    Example:
        delay_task = DelayTask(delay_seconds=5)
        delay_task.run()  # This will introduce a delay of 5 seconds.
    """

    def __init__(self, delay_seconds: float, **kwargs):
        """
        Initializes a new instance of the DelayTask class.

        Args:
            delay_seconds (float): The duration of the delay in seconds.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(**kwargs)
        self.delay_seconds = delay_seconds

    def run(self):
        """
        Runs the task. This method will introduce a delay in the task chain execution.

        The delay is introduced using the sleep function from the time module. The duration of the delay is specified by the delay_seconds attribute.

        The method also checks the status of the task during the delay. If the status changes to 'terminating', the delay is interrupted and the method exits.

        Once the delay is over or interrupted, the on_complete method is called to mark the task as complete or terminating respectively.

        Example:
            delay_task = DelayTask(delay_seconds=5)
            delay_task.run()  # This will introduce a delay of 5 seconds or less if the task is terminated earlier.
        """

        from datetime import datetime
        from time import sleep

        self.status = TaskStatusCodes.running
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < self.delay_seconds:
            sleep(1)

            if self.status == TaskStatusCodes.terminating:
                break

        self.on_complete()


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

    def _run(self) -> 'PruneTask':
        # If previous_task_data is True, clear the data of all previous tasks
        if self.previous_task_data:
            for i in range(self.task_chain.position):
                if hasattr(self.task_chain[i], 'data'):
                    setattr(self.task_chain[i], 'data', None)

        # If stored_variables is True, clear all variables stored in the task chain
        if self.stored_variables:
            self.task_chain.variables.clear()

        return self

    def on_complete(self) -> 'PruneTask':
        self.status = TaskStatusCodes.complete

        return self


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

    def run(self, *args, **kwargs) -> 'TemplateTask':
        self.status = TaskStatusCodes.running

        for record in self.records:
            from tasks.base import TaskConfiguration
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

        self.on_complete()

        return self


class WaitTask(BaseTask):
    """
    The WaitTask class is a subclass of the BaseTask class. It represents a task that waits for certain conditions to be met before it can be run.

    Attributes:
        chain (BaseTaskChain): The task chain that this task belongs to.
        position (int): The position of this task in the task chain.
        check_time_seconds (float): The time interval in seconds at which this task checks if its conditions are met.
        _when_all_previous_async_tasks_complete (bool): A flag indicating whether this task should wait for all previous async tasks to complete.
        _when_all_previous_tasks_complete (bool): A flag indicating whether this task should wait for all previous tasks to complete.
        _when_all_tasks_by_name_complete (List[str]): A list of task names. This task will wait until all tasks with these names are complete.
        _when_any_tasks_by_name_complete (List[str]): A list of task names. This task will wait until any task with these names is complete.
    """

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

    def on_complete(self):
        self.status = TaskStatusCodes.complete

    def run(self, *args, **kwargs):
        """
        Runs the task. This method will block until all conditions specified in the constructor are met.
        """

        from time import sleep

        try:
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
        finally:
            self.on_complete()

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
                if isinstance(task, BaseAsyncTask)
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

from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseTask, TaskStatusCodes
from typing import List


@register_definition(name='wait', category='task')
class WaitTask(BaseTask):
    def __init__(self,
                 check_time_seconds: float = 1,
                 when_after_seconds: float = 0,
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
            check_time_seconds (float, optional): The time interval in seconds at which this task checks if its conditions are met. Defaults to 1.
            when_after_seconds (float, optional): The time in seconds that this task should wait before proceeding. Defaults to 0.
            when_all_previous_async_tasks_complete (bool, optional): A flag indicating whether this task should wait for all previous async tasks to complete. Defaults to False.
            when_all_previous_tasks_complete (bool, optional): A flag indicating whether this task should wait for all previous tasks to complete. Defaults to False.
            when_all_tasks_by_name_complete (List[str], optional): A list of task names. This task will wait until all tasks with these names are complete. Defaults to None.
            when_any_tasks_by_name_complete (List[str], optional): A list of task names. This task will wait until any task with these names is complete. Defaults to None.
        """

        self.check_time_seconds = check_time_seconds
        self._when_after_seconds = when_after_seconds
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
                self.when_after_seconds,
                self.when_all_previous_async_tasks_complete,
                self.when_all_previous_tasks_complete,
                self.when_all_tasks_by_name_complete,
                self.when_any_tasks_by_name_complete,
                self.status == TaskStatusCodes.terminating
            ]):
                break

            sleep(self.check_time_seconds)

    @property
    def when_after_seconds(self) -> bool:
        """
        Checks if the allotted seconds have passed since this Task started. This method requires that super.on_start()
        is run at, at the least, that `self.start` is populated with a UTC datetime object.
        """

        from datetime import datetime, timezone

        if self._when_after_seconds > 0:
            if isinstance(self.start, datetime):
                return  (datetime.now(tz=timezone.utc) - self.start).total_seconds() > self._when_after_seconds

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
                for task in self.task_chain[0:self.position]
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
                for task in self.task_chain[0:self.position]
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
                for task in self.task_chain[0:self.position]
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
                for task in self.task_chain[0:self.position]
                if task.name in self._when_all_tasks_by_name_complete
            ])

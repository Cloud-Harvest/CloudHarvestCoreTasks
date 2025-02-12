"""
This module contains subclasses of BaseTaskChain that are used to represent different types of task chains.
"""

from CloudHarvestCorePluginManager.decorators import register_definition
from .base import BaseTaskChain


@register_definition(name='report', category='chain')
class ReportTaskChain(BaseTaskChain):
    """
    A class used to represent a Report Task Chain.

    This class is a subclass of the BaseTaskChain class and is used to manage a sequence of tasks related to generating reports.

    Attributes
    ----------
    headers: list
        A list of headers to be used in the report.

    Methods
    -------
    run() -> None:
        Executes all the tasks in the task chain in the order they were added. This method will block until all tasks are completed.
    add_task(task: BaseTask) -> None:
        Adds a task to the end of the task chain.
    remove_task(task: BaseTask) -> None:
        Removes a task from the task chain.
    """

    def __init__(self, *args, **kwargs):
        """
        Constructs all the necessary attributes for the ReportTaskChain object.

        Parameters
        ----------
            *args:
                Variable length argument list.
            **kwargs:
                Arbitrary keyword arguments.
        """
        self.headers = kwargs.get('template', {}).get('headers') or None

        super().__init__(*args, **kwargs)

    def run(self) -> 'ReportTaskChain':
        super().run()

        # Add the headers to the metadata
        self.meta['headers'] = self.headers

        return self

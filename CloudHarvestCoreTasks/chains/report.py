from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.chains.base import BaseTaskChain


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

        super().__init__(*args, **kwargs)

        # Sends the headers back to the client as part of the response
        self.meta['headers'] = self.headers

    def run(self) -> 'ReportTaskChain':
        super().run()

        return self

    @property
    def headers(self) -> list:
        # If the task chain has headers, we use them as the default headers for the task
        headers = self.filters.get('headers', []) or self.original_template.get('headers') or []
        add_keys = self.filters.get('add_keys', []) or []
        exclude_keys = self.filters.get('exclude_keys', []) or []

        return [
            header for header in (headers + add_keys)
            if header not in exclude_keys
        ]

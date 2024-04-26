from .base import BaseTaskChain


class ReportTaskChain(BaseTaskChain):
    """
    A class used to represent a Report Task Chain.

    This class is a subclass of the BaseTaskChain class and is used to manage a sequence of tasks related to generating reports.

    Attributes
    ----------
    tasks : list
        The list of tasks to be executed in this task chain. Each task is an instance of a subclass of the BaseTask class.

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

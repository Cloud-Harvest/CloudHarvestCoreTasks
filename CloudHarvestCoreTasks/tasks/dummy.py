from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseTask


@register_definition(name='dummy', category='task')
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
        self.result = [{'dummy': 'data'}]
        self.meta = {'info': 'this is dummy metadata'}

        return self

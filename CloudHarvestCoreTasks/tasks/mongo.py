from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.filters import MongoFilter
from CloudHarvestCoreTasks.tasks.base import BaseDataTask, BaseFilterableTask
from exceptions import TaskException

from pymongo import MongoClient


@register_definition(name='mongo', category='task')
class MongoTask(BaseDataTask, BaseFilterableTask):
    """
    The MongoTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a MongoDB database.
    """

    def __init__(self, collection: str = None, result_attribute: str = None, *args, **kwargs):
        """
        Initializes a new instance of the MongoTask class.

        Args:
            collection (str, optional): The name of the collection to interact with. When not provided, database-level commands are exposed.
            result_attribute (str, optional): The attribute to retrieve from the result.
            *args: Variable length argument list passed to the parent class.
            **kwargs: Arbitrary keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)

        self.collection = collection
        self.result_attribute = result_attribute

    def apply_filters(self) -> 'MongoTask':
        """
        Applies user filters to the database configuration.
        """

        if self.filters.accepted is None or self.ignore_filters:
            return self

        with MongoFilter() as mongo_filter:
            mongo_filter.pipeline = self.arguments.get('pipeline')
            mongo_filter.apply()

        return self

    def method(self, *args, **kwargs):
        """
        Runs the task. This method will execute the method defined in `self.command` on the database or collection and
        store the result in the result attribute. `self.result_attribute` is used to extract the desired attribute
        from the result, if applicable.
        """

        # If connected, return existing connection otherwise connect
        # from pymongo import MongoClient
        # silo_config = self.silo.__dict__()
        # silo_config.pop('engine')
        # silo_config.pop('database')
        #
        # silo_extend = silo_config.pop('extended_db_configuration', {})
        # connection_config = silo_config | silo_extend
        # client = MongoClient(**connection_config)
        # si = client.server_info()

        client: MongoClient = self.silo.connect()

        if not self.silo.is_connected:
            raise TaskException(self, f'Unable to connect to the {self.silo.name} silo.')

        if self.collection:
            # Note that MongoDb does not return an error if a collection is not found. Instead, MongoDb will faithfully
            # create the new collection name, even if it malformed or incorrect. This is an intentional feature of MongoDb.
            database_object = client[self.silo.database][self.collection]

        else:
            # Expose database-level commands
            database_object = client[self.silo.database]

        # Execute the command on the database or collection
        self.calls += 1

        result = self.walk_result_command_path(
            getattr(database_object, self.base_command_part)(**self.arguments)
        )

        # Convert the result to a list if it is a generator or cursor
        from types import GeneratorType
        from pymongo import CursorType
        from pymongo.cursor import Cursor

        if isinstance(result, (GeneratorType, CursorType, Cursor)):
            result = list(result)

        # Record the result
        self.result = result

        return self

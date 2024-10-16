"""
tasks.py - This module contains classes for various tasks that can be used in task chains.

A Task must be registered with the CloudHarvestCorePluginManager in order to be used in a task chain. This is done by
decorating the Task class with the @register_definition decorator. The name of the task is specified in the decorator
and is used to reference the task in task chain configurations.

Tasks are subclasses of the BaseTask class, which provides common functionality for all tasks. Each task must implement
a method() function that performs the task's operation. The method() function should return the task instance.
"""

from CloudHarvestCorePluginManager.decorators import register_definition
from typing import Any, List, Literal

from .base import (
    BaseDataTask,
    BaseTask,
    BaseTaskChain,
    BaseTaskException,
    TaskStatusCodes
)

from ..user_filters import MongoUserFilter

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


@register_definition(name='error', category='task')
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


@register_definition(name='file', category='task')
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
                 mode: Literal['append', 'read', 'write'],
                 desired_keys: List[str] = None,
                 format: Literal['config', 'csv', 'json', 'raw', 'yaml'] = None,
                 template: str = None,
                 *args, **kwargs):

        """
        Initializes a new instance of the FileTask class.

        Args:
            path (str): The path to the file.
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
        self.format = str(format or self.determine_format()).lower()
        self.desired_keys = desired_keys
        self.template = template

        # Value Checks
        if self.mode == 'read':
            if self.result_as is None:
                raise ValueError(f'{self.name}: The `result_as` attribute is required for read operations.')

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
                        self.result = {k: v for k, v in result.items() if k in self.desired_keys}

                    elif isinstance(result, list):
                        self.result = [{k: v for k, v in record.items() if k in self.desired_keys} for record in result]

                # Return the entire result
                else:
                    self.result = result

            # Write operations
            else:

                try:
                    # If the user has specified desired_keys, filter the data to just those keys, if applicable
                    if self.desired_keys:
                        from flatten_json import flatten, unflatten
                        separator = '.'

                        # If the data is a dictionary, flatten it, filter the keys, and unflatten it
                        if isinstance(self.data, dict):
                            self.data = unflatten({
                                k: v for k, v in flatten(self.data.items, separator=separator)()
                                if k in self.desired_keys
                            }, separator=separator)

                        # If the data is a list, flatten each record, filter the keys, and unflatten each record
                        elif isinstance(self.data, list):
                            self.data = [
                                unflatten({
                                    k: v for k, v in flatten(record, separator=separator).items()
                                    if k in self.desired_keys
                                })
                                for record in self.data
                            ]

                    if self.format == 'config':
                        config = ConfigParser()
                        if isinstance(self.data, dict):
                            config.read_dict(self.data)

                            config.write(file)

                        else:
                            raise BaseTaskException(f'{self.name}: `FileTask` only supports dictionaries for writes to config files.')

                    elif self.format == 'csv':
                        if isinstance(self.data, list):
                            if all([isinstance(record, dict) for record in self.data]):
                                consolidated_keys = set([key for record in self.data for key in record.keys()])
                                use_keys = self.desired_keys or consolidated_keys

                                writer = DictWriter(file, fieldnames=use_keys)
                                writer.writeheader()

                                writer.writerows(self.data)

                                return self

                        raise BaseTaskException(f'{self.name}: `FileTask` only supports lists of dictionaries for writes to CSV files.')

                    elif self.format == 'json':
                        json.dump(self.data, file, default=str, indent=4)

                    elif self.format == 'yaml':
                        yaml.dump(self.data, file)

                    else:
                        file.write(str(self.data))
                finally:
                    from os.path import exists
                    if not exists(self.abs_path):
                        raise FileNotFoundError(f'{self.name}: The file `{file}` was not written to disk.')

        return self


@register_definition(name='recordset', category='task')
class HarvestRecordSetTask(BaseTask):
    """
    The HarvestRecordSetTask class is a subclass of the BaseTask class. It represents a task that operates on a record set.

    Attributes:
        data (Any): The record set to operate on.
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



    def __init__(self, data: Any, stages: List[dict], *args, **kwargs):
        """
        Constructs a new HarvestRecordSetTask instance.

        Args:
            stages (List[dict]): A list of dictionaries containing the function name and arguments to be applied to the recordset.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        from ..data_model.recordset import HarvestRecordSet
        self.data = data if isinstance(data, HarvestRecordSet) else HarvestRecordSet(data=data)
        self.stages = stages
        self.stage_position = 0

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

        from ..data_model.recordset import HarvestRecordSet, HarvestRecord
        for stage in self.stages:
            # Each dictionary should only contain one key-value pair
            for function, arguments in stage.items():

                # This is a HarvestRecordSet command
                if hasattr(HarvestRecordSet, function):
                    # We don't template RecordSet commands because they are not intended to be used with record-level data
                    getattr(self.data, function)(**(arguments or {}))

                # This is a HarvestRecord command which must iterate over each record in the record set
                elif hasattr(HarvestRecord, function):
                    for record in self.data:
                        # Here, we use record-level templating to allow for dynamic arguments based on the record
                        from .templating import template_object

                        # We can't used items() here because we do not iterate over the dictionary
                        templated_stage = template_object(template=self.original_template['stages'][self.stage_position],
                                                          variables=record)

                        # Execute the function on the record
                        getattr(record, function)(**(list(templated_stage.values())[0] or {}))

                else:
                    raise AttributeError(f"Neither HarvestRecordSet nor HarvestRecord has a method named '{function}'")

                break

            # Increment the stage_position
            self.stage_position += 1

        self.result = self.data

        return self


@register_definition(name='prune', category='task')
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
                if str(self.task_chain[i].status) in [str(TaskStatusCodes.complete), str(TaskStatusCodes.error), str(TaskStatusCodes.skipped)]:
                    total_bytes_pruned += getsizeof(self.task_chain[i].result)
                    self.task_chain[i].result = None

        # If stored_variables is True, clear all variables stored in the task chain
        if self.stored_variables:
            total_bytes_pruned += getsizeof(self.task_chain.variables)
            self.task_chain.variables.clear()

        self.result = {
            'total_bytes_pruned': total_bytes_pruned
        }

        return self

@register_definition(name='json', category='task')
class JsonTask(BaseTask):
    def __init__(self, mode: Literal['serialize', 'deserialize'], data: Any = None, default_type: type = str,
                 parse_datetimes: bool = False, *args, **kwargs):
        """
        Initializes a new instance of the JsonTask class.

        Args:
            data (Any, optional): The data to load or dump. Defaults to None.
            mode (Literal['serialize', 'deserialize']): The mode in which to operate. 'load' reads a JSON file, 'dump' writes a JSON file.
            default_type (type, optional): The default type to use when loading JSON data. Defaults to str.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.data = data
        self.mode = mode
        self.default_type = default_type
        self.parse_datetimes = parse_datetimes

    def method(self):
        """
        Executes the task.

        Returns:
            JsonTask: The current instance of the JsonTask class.
        """
        def do_mode(data: Any):
            import json
            if self.mode == 'deserialize':
                # Make sure the data is a string, otherwise it has already been deserialized
                if isinstance(data, str):
                    deserialized = json.loads(data)

                else:
                    deserialized = data

                if self.parse_datetimes:
                    def parse_datetime(v: Any):
                        """
                        Attempts to parse a string as a datetime object. If the string cannot be parsed, it is returned as-is.
                        """
                        from datetime import datetime

                        try:
                            return datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')

                        except Exception as ex:
                            return v

                    if isinstance(deserialized, dict):
                        for key, value in deserialized.items():
                            deserialized[key] = parse_datetime(value)

                    elif isinstance(deserialized, list):
                        for i, item in enumerate(deserialized):
                            deserialized[i] = parse_datetime(item)

                    else:
                        deserialized = parse_datetime(deserialized)

                return deserialized


            # Convert the data into a string
            elif self.mode == 'serialize':
                # default=str is used to serialization values such as datetime objects
                # This can lead to inconsistencies in the output, but it is necessary

                return json.dumps(data, default=str)


        # Check if self.data is an iterable
        if isinstance(self.data, (list, tuple)):
            self.result = [do_mode(d) for d in self.data]

        else:
            self.result = do_mode(self.data)

        return self


@register_definition(name='mongo', category='task')
class MongoTask(BaseDataTask):
    """
    The MongoTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a MongoDB database.
    """
    from ..user_filters import MongoUserFilter
    from pymongo import MongoClient

    CONNECTION_POOLS = {}
    REQUIRED_CONFIGURATION_KEYS = ['host', 'port', 'database']
    USER_FILTER_CLASS = MongoUserFilter
    USER_FILTER_STAGE = 'start'

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

        if self.alias is not None:
            if self.alias == 'persistent':
                from ..silos.persistent import connect
                self._connection = connect(database=self.database)

            else:
                raise ValueError(f"Invalid alias '{self.alias}' for MongoTask. Must be 'persistent'.")

    @property
    def is_connected(self) -> bool:
        """
        Checks if the connection to the MongoDB server is active.
        """
        try:
            self._connection.server_info()
            return True

        except Exception:
            return False

    def apply_user_filters(self) -> 'BaseTask':
        """
        Applies user filters to the database configuration.
        """
        if self.user_filters.get('accepted') is None:
            return self

        pipeline = self.arguments.get('pipeline')
        with MongoUserFilter(pipeline=pipeline, **self.user_filters) as ufc:
            ufc.apply()

            if pipeline:
                self.arguments['pipeline'] = ufc.result

            else:
                self.arguments = ufc.result

    def connect(self) -> MongoClient:
        """
        Connects to the MongoDB server. If an existing pool is available, it will be used. Otherwise, a new connection
        pool will be created and stored for future use.
        """

        # If already connected, return the existing connection
        if self.is_connected:
            return self._connection

        # Create a connection pool key based on the database configuration
        connection_pool_key = self.connection_sha

        # If a connection pool exists, use it
        if self.CONNECTION_POOLS.get(connection_pool_key):
            self._connection = self.CONNECTION_POOLS[connection_pool_key]

        # Otherwise, create a new connection pool
        else:
            from pymongo import MongoClient
            self._connection = MongoClient(**{'maxPoolSize': self.max_pool_size} | self.mapped_connection_configuration)
            self.CONNECTION_POOLS[connection_pool_key] = self._connection

        return self._connection

    def disconnect(self):
        """
        Disconnects from the MongoDB server.
        """
        try:
            self._connection.close()

        except Exception:
            pass

    def method(self, *args, **kwargs):
        """
        Runs the task. This method will execute the method defined in `self.command` on the database or collection and
        store the result in the result attribute. `self.result_attribute` is used to extract the desired attribute
        from the result, if applicable.
        """

        # If connected, return existing connection otherwise connect
        super().connect()

        if self.collection:
            # Note that MongoDb does not return an error if a collection is not found. Instead, MongoDb will faithfully
            # create the new collection name, even if it malformed or incorrect. This is an intentional feature of MongoDb.
            database_object = self._connection[self.database][self.collection]

        else:
            # Expose database-level commands
            database_object = self._connection[self.database]

        # Execute the command on the database or collection
        result = getattr(database_object, self.command)(**self.arguments)

        # Extract the desired attribute from the result, if applicable
        if self.result_attribute:
            result = getattr(result, self.result_attribute)

        # Record the result
        self.result = result

        return self

@register_definition(name='redis', category='task')
class RedisTask(BaseDataTask):
    """
    The RedisTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a Redis database.

    If a Redis command does not accept any of the VALID_READ_KEYS, then that command cannot be used with the RedisTask.

    >>> task = RedisTask(
    >>>     command='get',
    >>>     arguments={'key': 'my_key'}
    >>> )
    """
    from redis import StrictRedis

    # The connection key map is used to map the connection attributes to the appropriate attributes in the subclass.
    # base_configration_key: The attribute in the BaseDataTask class.
    # driver_configuration_key: The attribute specific to the data provider driver / module.
    # Format: (base_configuration_key, driver_configuration_key)

    CONNECTION_KEY_MAP = (
        ('host', 'host'),
        ('port', 'port'),
        ('username', 'username'),
        ('password', 'password'),
        ('database', 'db'),
    )

    # Connection pools are used to store connections to the data provider. This reduces the number of connections to the
    # data provider and allows us to reuse connections. Override this attribute in subclasses to provide data provider
    # specific connection pools.
    CONNECTION_POOLS = {}

    # Default connection pool parameters are used to provide default values for the connection pool.
    DEFAULT_CONNECTION_POOL_PARAMETERS = {
        'decode_responses': True
    }

    # The following keys are used to validate the arguments provided to the RedisTask. If a Redis command does not accept
    # any of these keys, then that command cannot be used with the RedisTask.
    VALID_READ_KEYS = ('key', 'keys', 'name', 'pattern')

    def __init__(self, expire: int = None, serialization: bool = False, *args, **kwargs):
        """
        Initializes a new instance of the RedisTask class.

        If the alias is 'ephemeral', the connection will be created using the ephemeral connection method. Otherwise, the
        connection will be created using the host, port, username, password, and database attributes.

        Args
        expire (int, optional): The expiration time for records in the Redis database. Defaults to None.
        serialization (bool, optional): When True: data being written will be serialized while data read will be deserialized. Defaults to False.

        """

        # Initialize the BaseDataTask class
        super().__init__(*args, **kwargs)

        self.expire = expire
        self.serialization = serialization

        # Validate the RedisTask configuration
        if not hasattr(self, f'redis_{self.command}'):
            raise ValueError(f"Invalid command '{self.command}' for RedisTask. Got '{self.command}'.")

        # Make sure that self.alias is 'ephemeral' if it is provided
        if self.alias and self.alias != 'ephemeral':
            raise ValueError(f"Invalid alias '{self.alias}' for RedisTask. Must be 'ephemeral'.")

        elif not self.alias and not self.host:
            raise ValueError(f"Missing required configuration 'host' for RedisTask.")

        # Make sure that the command is a valid Redis connector command
        from redis import StrictRedis
        if not hasattr(StrictRedis, self.command):
            raise ValueError(f"Invalid command '{self.command}' for RedisTask. Got '{self.command}'."
                             f"\nCheck the Redis documentation at https://redis-py.readthedocs.io/en/stable/commands.html.")

    @property
    def is_connected(self) -> bool:
        try:
            self.connection.ping()
            return True

        except Exception:
            return False

    def connect(self) -> StrictRedis:
        """
        Connects to a Redis server. If an existing pool is available, it will be used. Otherwise, a new connection
        pool will be created and stored for future use.
        """
        from redis import ConnectionPool, StrictRedis

        # If already connected, return the existing connection
        if self.is_connected:
            return self.connection

        # If the alias is 'ephemeral', connect to the Redis database using the ephemeral connection method
        if self.alias == 'ephemeral':
            from ..silos.ephemeral import connect

            self.connection = connect(database=self.database)
            return self.connection

        # Create a connection pool key based on the connection configuration
        connection_pool_key = self.connection_pool_key

        # If a connection pool exists, get a connection from the pool
        if self.CONNECTION_POOLS.get(connection_pool_key):
            connection_pool = self.CONNECTION_POOLS[connection_pool_key]

        # Otherwise, create a new connection pool
        else:
            connection_pool = ConnectionPool(
                **(
                        self.DEFAULT_CONNECTION_POOL_PARAMETERS |        # Default connection pool parameters
                        {'max_connections': self.max_pool_size} |        # (super) Maximum number of connections in the pool
                        self.mapped_connection_configuration             # (super) Connection configuration mapped to the driver
                )
            )

            self.CONNECTION_POOLS[connection_pool_key] = connection_pool

        # Assign the connection
        self.connection = StrictRedis(connection_pool=connection_pool)

        return self.connection

    def disconnect(self):
        self.connection.close()

    def method(self, *args, **kwargs) -> 'RedisTask':
        """
        Executes the command on the Redis database, handling pattern or keys iteration if necessary. This is accomplished
        by checking if the provided 'pattern' or 'keys' arguments are present in the command's signature. If they are not,
        the command is executed as-is. If they are, the command is executed for each key in the pattern or keys.
        """

        self.connect()

        self.result = getattr(self, f'redis_{self.command}')()

        return self

    def redis_delete(self) -> dict:
        """
        Deletes the records from the Redis database based on a list of 'keys' or a 'pattern'.

        Configuration Example
        ```yaml
        - name: delete_redis_keys
          alias: ephemeral
          command: delete
          keys:
            - key1
            - key2

        - name: delete_redis_keys
          alias: ephemeral
          command: delete
          pattern: "key*"
        ```
        """

        # Retrieve keys based on the pattern or keys provided
        keys = self.redis_keys()

        delete_count = self.connection.delete(*keys)

        result = {
            'deleted': delete_count,
            'keys': keys
        }

        return result

    def redis_expire(self) -> list:
        """
        Sets the expiration time for records in the Redis database based on a list of keys or a pattern.

        Configuration Example
        ```yaml
        - name: expire_redis_keys
          alias: ephemeral
          command: expire
          keys:
            - key1
            - key2
        ```

        ```yaml
        - name: expire_redis_keys
          alias: ephemeral
          command: expire
          pattern: "key*"
        ```
        """

        keys = self.redis_keys()

        for key in keys:
            self.calls += 1
            self.connection.expire(name=key, time=self.expire)

        self.result = {'keys': keys}

        return keys

    def redis_flush(self):
        """
        Removes all records from the Redis database. This action is not recommended and may result in data loss.
        Instead, make sure to expire records when they are no longer needed.

        Configuration Example
        ```yaml
        - name: flush_redis
          alias: ephemeral
          command: flush
        ```
        """

        self.calls += 1

        delete_count = self.connection.flushall()

        result = {
            'deleted': delete_count
        }

        return result

    def redis_get(self) -> list:
        """
        Gets the records from the Redis database based on a list of keys or a pattern. Returns a list of records. The
        return for this function is always a list of dictionaries. '_name' is included in the dictionary to indicate the
        name of the record.

        Configuration Example
        ```yaml
        - name: get_redis_records
          alias: ephemeral
          command: get
          keys:
            - key1
            - key2
        ```

        ```yaml
        - name: get_redis_records
          alias: ephemeral
          command: get
          pattern: "key*"
        ```
        """

        results = []

        # When 'name' and 'keys' are provided, we treat this as an hget() operation
        if self.arguments.get('name') and self.arguments.get('keys'):
            name = self.arguments.get('name')

            # When 'keys' is '*', we treat this as an hgetall() operation and retrieve all keys for the 'name'
            if self.arguments.get('keys') == '*':
                self.calls += 1
                result = self.connection.hgetall(name=name)

            # Otherwise, 'keys' is a list and we should only retrieve those keys in question
            else:
                keys = self.arguments.get('keys')
                result = {}
                for key in keys:
                    self.calls += 1

                    try:
                        v = self.connection.hget(name=self.arguments['name'], key=key)

                        if self.serialization:
                            from json import loads
                            v = loads(v)

                    except Exception as ex:
                        v = None

                    result[key] = v

                result['_name'] = name
                results.append(result)

        # If 'name' and 'keys' is not provided, we will use 'keys' or 'pattern' to retrieve the records
        else:
            keys = self.redis_keys()

            results = []

            for key in keys:
                self.calls += 1

                try:
                    result = self.connection.get(name=key)

                    if self.serialization:
                        from json import loads
                        result = loads(result)

                except Exception as ex:
                    result = None

                result['_name'] = key
                results.append(result)

        return results

    def redis_keys(self) -> list:
        """
        Gets the keys from the Redis database based on a pattern. If 'keys' are provided, the keys are returned as-is.

        Configuration Example
        ```yaml
        - name: get_redis_keys
          alias: ephemeral
          command: keys
          pattern: "key*"
        ```
        """

        # Use pattern matching to return a list of keys
        if self.arguments.get('pattern'):
            result = self.connection.scan_iter(match=self.arguments['pattern'])

        # If the keys are provided, return the keys
        elif self.arguments.get('keys'):
            result = self.arguments.get('keys')

        # Return all keys
        else:
            result = self.connection.scan_iter(match='*')

        return result

    def redis_set(self):
        """
        Sets the records in the Redis database based on a dictionary of keys and values. How records are added is based
        on the presence of 'name' and 'key' in the arguments.

        Configuration Example
        ```yaml
        - name: set_redis_keys
          alias: ephemeral
          command: set
          data:
            key1: value1
            key2: value2
        ```
        """

        _STRINGABLE = (str or int or float)

        results = {
            'added': 0,
            'errors': 0,
            'updated': 0
        }

        def _set(n: str = None, k: str = None, v: _STRINGABLE = None):
            """
            Sets the value in the Redis database. Accepts a name, key, and value. If a serialization is required, the
            value will be serialized before being set. If an expiration time is provided, the expiration time will be set
            for the record.

            * If 'name' and 'keys' are provided
                - hset() is used to set the value for each key

                * AND the data is a dictionary
                    - the data is treated as a mapping of key/value pairs belonging to 'name'.

                * AND the data is a list
                    - each iteration of the list is treated as a dictionary of key/value pairs belonging to the value
                      of the 'name' field.

            * If only 'name' and 'value' are provided and the data is a dictionary


            """
            # Serialize the value if necessary
            if self.serialization and not isinstance(v, _STRINGABLE):
                from json import dumps
                v = dumps(v, default=str)

            try:
                self.calls += 1

                if n and k:
                    # Use hset() if a name and key are provided
                    r = self.connection.hset(name=n, key=k, value=v)

                    # If an expiration time is provided, set the expiration time for this record
                    if self.expire:
                        self.calls += 1
                        self.connection.expire(name=n, time=self.expire)

                else:
                    # Use set() if only a name is provided. Include expiration time (ex) if provided.
                    r = self.connection.set(name=n, value=v, ex=self.expire)

            except Exception as ex:
                results['errors'] += 1

            else:
                return r

        # This is an hset() operation
        if self.arguments.get('name') and self.arguments.get('key'):
            # If a dictionary is provided, we treat 'name' as a static value and iterate over the key/value pairs
            if isinstance(self.data, dict):
                for key, value in self.data.items():
                    _set(n=self.arguments['name'], k=key, v=value)

            # If a list is provided, 'name' is treated as a key reference in the data
            elif isinstance(self.data, list):
                for item in self.data:
                    name = item['name']
                    if isinstance(item, dict):
                        for key, value in item.items():
                            _set(n=name, k=key, v=value)

        # When only a 'name' and 'value' are provided, we treat this as a set() operation
        else:
            # If a dictionary is provided, we treat 'name' as a dynamic value based on the dictionary key
            # and the value as the dictionary value
            if isinstance(self.data, dict):
                for name, value in self.data.items():
                    _set(n=name, v=value)

            # If a list is provided, we treat 'name' and 'value' as key references in the data
            elif isinstance(self.data, list):
                for item in self.data:
                    if isinstance(item, dict):
                        _set(n=item.get(self.arguments['name']),
                             v=item.get(self.arguments['value']))

            elif isinstance(self.data, _STRINGABLE):
                # TODO: finish this nonsense
                pass

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

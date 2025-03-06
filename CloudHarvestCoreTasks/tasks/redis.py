from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseDataTask
from exceptions import TaskException

from typing import Any


@register_definition(name='redis', category='task')
class RedisTask(BaseDataTask):
    """
    The RedisTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a
    Redis database.

    The Redis Python's connection class is not directly accessible via this DataTask. Instead, we provide some common
    database operations as methods named 'redis_' followed by the function, such as 'redis_get'.

    >>> task = RedisTask(
    >>>     command='get',
    >>>     arguments={'key': 'my_key'}
    >>> )
    """

    # These are the data types permitted in Redis. We use this list to evaluate if a value must be serialized before
    # being written to the Redis database.
    VALID_REDIS_TYPES = (str or int or float)

    def __init__(self, expire: int = None, serialization: bool = False, *args, **kwargs):
        """
        Initializes a new instance of the RedisTask class.

        Args:
        expire (int, optional): The expiration time for records in the Redis database. Defaults to None.
        serialization (bool, optional): When True: data being written will be serialized while data read will be deserialized. Defaults to False.

        """

        # Initialize the BaseDataTask class
        super().__init__(*args, **kwargs)

        self.expire = expire
        self.serialization = serialization

        # Validate the RedisTask configuration
        if not hasattr(self, f'redis_{self.command}'):
            methods = [
                method[6:] for method in dir(self)
                if method.startswith('redis_')
            ]

            raise TaskException(self, f"Invalid command '{self.command}' for RedisTask. Must be one of {methods}.")

    def method(self) -> 'RedisTask':
        """
        Executes the 'self.command' on the Redis database. Each offered StrictRedis command is prefixed with 'redis_'.
        See the individual methods for more information.

        It was necessary to break each offered command into different methods due to the complexity
        of the Redis api and the need to serialize and deserialize data.
        """

        result = self.walk_result_command_path(
            getattr(self, f'redis_{self.base_command_part}')()
        )

        self.result = result

        return self

    def redis_delete(self) -> dict:
        """
        Deletes the records from the Redis database based on a list of 'keys' or a 'pattern'.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.delete

        Arguments
        keys (List[str], optional): A list of keys to delete. Defaults to None.
        pattern (str, optional): A pattern to match keys. Defaults to None.

        Example:
        >>> # Delete records with keys 'key1' and 'key2'
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'delete',
        >>>         'arguments': {
        >>>             'keys': ['key1', 'key2']
        >>>             }
        >>>     }
        >>> }
        >>>
        >>> # Delete keys based on a pattern
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'delete',
        >>>         'arguments': {
        >>>             'pattern': 'key*'
        >>>             }
        >>>     }
        >>> }
        >>>
        >>> # Delete all keys
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'delete',
        >>>     }
        >>> }

        """

        # Retrieve keys based on the pattern or keys provided
        keys = self.redis_keys()

        delete_count = self.silo.connect().delete(*keys)

        result = {
            'deleted': delete_count,
            'keys': keys
        }

        return result

    def redis_expire(self) -> list:
        """
        Sets the expiration time for records in the Redis database based on a list of keys or a pattern.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.expire

        Arguments
        expire (int): The expiration time in seconds.
        keys (List[str], optional): A list of keys to expire. Defaults to None.
        pattern (str, optional): A pattern to match keys. Defaults to None.

        Example:
        >>> # Delete records with keys 'key1' and 'key2'
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'expire',
        >>>         'arguments': {
        >>>             'expire': 3600,         # (seconds) Expire in 1 hour
        >>>             'keys': ['key1', 'key2']
        >>>             }
        >>>     }
        >>> }
        """

        keys = self.redis_keys()

        for key in keys:
            self.calls += 1
            self.silo.connect().expire(name=key, time=self.arguments['expire'])

        self.result = {'keys': keys}

        return keys

    def redis_flushall(self):
        """
        Removes all records from the Redis database. This action is not recommended and may result in data loss.
        Instead, make sure to expire records when they are no longer needed.

        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.flushall

        Example:
        >>> # Delete all records from the database
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'flushall',
        >>>     }
        >>> }

        """

        self.calls += 1

        delete_count = self.silo.connect().flushall()

        result = {
            'deleted': delete_count
        }

        return result

    def redis_get(self) -> list:
        """
        Gets the records from the Redis database based on a list of keys or a patterns. Returns a list of records. The
        return for this function is always a list of dictionaries. '_name' is included in the dictionary to indicate the
        name of the record.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.get

        Arguments
        names (str or List[str], optional): One or a list of names to retrieve. Defaults to None.
        keys (str or List[str], optional): One or a list of keys to retrieve. Defaults to None. Requires 'names' or 'patterns'.
        patterns (str or List[str], optional): One or a list of patterns to match keys. Defaults to None.

        Example:
        >>> # Retrieve records named 'key1' and 'key2'
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'get',
        >>>         'arguments': {
        >>>             'names': ['key1', 'key2']
        >>>             }
        >>>     }
        >>> }

        >>> # Retrieve records based on a patterns
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'get',
        >>>         'arguments': {
        >>>             'patterns': ['key*']
        >>>             }
        >>>     }
        >>> }
        """

        results = []

        def _get(n: str) -> dict:
            """
            Performs a simple get() operation on the Redis database.

            Arguments
            n (str): The name of the key to retrieve.

            Returns
            dict: The record retrieved from the Redis database. When the record is a simple value, it is stored in the 'value' field.
            """

            try:
                self.calls += 1

                r = self.silo.connect().get(name=n)

                if self.serialization:
                    from json import loads
                    r = loads(r)

            except Exception as ex:
                self.meta['Errors'].append(f"Error retrieving key '{n}': {str(ex)}")
                raise TaskException(self, f"Error retrieving key '{n}': {str(ex)}")

            else:
                if isinstance(r, dict):
                    r['_id'] = n

                else:
                    r = {
                        '_id': n,
                        'value': r
                    }

                return r

        # List of names
        names = self.arguments.get('names')
        if isinstance(names, str):
            names = [names]

        # List of keys
        keys = self.arguments.get('keys')
        if keys and isinstance(keys, str):
            keys = [keys]

        # Patterns to match keys
        patterns = self.arguments.get('patterns')
        if patterns and isinstance(patterns, str):
            patterns = [patterns]

        # HGET operations combine NAMES/PATTERN with a list of KEYS
        if (names and keys) or (patterns and keys):
            names = self.redis_keys() if patterns else names

            for name in names:
                if keys == ['*']:
                    # HGETALL operation returns all keys for the given name
                    self.calls += 1
                    result = self.silo.connect().hgetall(name=name)

                else:
                    # HGET operation
                    result = {}
                    for key in keys:
                        self.calls += 1
                        result[key] = self.silo.connect().hget(name=name, key=key)

                # Deserialize the result if necessary
                if self.serialization:
                    from json import loads, JSONDecodeError

                    for key, value in result.items():
                        try:
                            result[key] = loads(value)

                        except JSONDecodeError:
                            result[key] = value

                # Add the name field to the record
                result['_id'] = name

                # Append this name result to the results list
                results.append(result)

        # GET operations
        elif patterns:
            # Retrieve the record names based on the pattern
            names = self.redis_keys()

            # Get the records
            [
                results.append(_get(n=name))
                for name in names
            ]

        elif names:
            # Get operation
            [
                results.append(_get(n=name))
                for name in names
            ]

        else:
            raise TaskException(self, f'Invalid arguments. Correct argument combinations are: (names, keys), (names, patterns), (names), (patterns), (keys).')

        return results

    def redis_keys(self) -> list:
        """
        Gets the keys from the Redis database based on a pattern. If 'keys' are provided, the keys are returned as-is.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.keys

        Example
        >>> # Returns a list of keys based on a pattern
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'keys',
        >>>         'arguments': {
        >>>             'pattern': 'key*'
        >>>             }
        >>>     }
        >>> }
        >>>
        >>> # Returns a list of all keys in the database
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'keys'
        >>>     }
        >>> }
        """

        # Use pattern matching to return a list of keys
        if self.arguments.get('patterns'):
            patterns = self.arguments.get('patterns')

            if isinstance(patterns, str):
                patterns = [patterns]

            result = []
            for pattern in patterns:
                result += self.silo.connect().scan_iter(match=pattern)

        # If the keys are provided, return the keys
        elif self.arguments.get('keys'):
            result = self.arguments.get('keys')

        # Return all keys
        else:
            result = self.silo.connect().scan_iter(match='*')

        return result

    def redis_set(self):
        """
        Writes records to the Redis database. If the record already exists, it will be overwriten.

        This method operates in two modes: SET and HSET. The mode is determined by the keys provided by the Task
        Configuration.

        Providing 'name' and 'value' implements 'SET'.
        Providing 'name' and 'keys' uses 'HSET'.

        Note that 'item.key_name' returns the value of that key, not the name of the key. If a static name should be
        provided, use a string which does not begin with 'var.' or 'item.' which reference the TaskChain's variables.

        SET
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.set
        This is used when the data consists of a simple name/value pair. In the following example, we use a simple
        dictionary of 'name' and 'value' which are then stored in the database. 'var_my_record' represents a variable
        stored in TaskChain.variables.
        >>> # Represents a variable 'my_record' stored in 'TaskChain.variables'.
        >>> var_my_record = {'name': 'myname', 'value': 'myvalue'}
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'arguments': {
        >>>             'name': 'var.my_record.name',                # 'myname'
        >>>             'value': 'var.my_record.value'               # 'myvalue'
        >>>         }
        >>>     }
        >>> }

        Use the 'iteration' directive when there is a list of records which needs to be stored. In this example, two new
        RedisTasks will be created, one for each record.
        >>> # Represents a variable 'my_records' stored in 'TaskChain.variables'.
        >>> var_my_records = [{'name': 'Bob', 'age': 28}, {'name': 'Susan', 'age': 30}]
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'arguments': {
        >>>             'name': 'item.name',        # Bob, Susan
        >>>             'value': 'item.age'         # 28, 30
        >>>         },
        >>>         'iteration': 'var.my_records'
        >>>     }
        >>> }

        HSET
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.cluster.RedisClusterCommands.hset
        This command is used when the data consists of 'name' and 'keys', where 'keys' is a list of keynames matching
        the desired keys to be stored in the database. The special value of '*' can be given for all keys.

        >>> # Represents a variable 'my_record' stored in 'TaskChain.variables'.
        >>> var_my_record = {'name': 'myname', 'value': 'myvalue'}
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'data': 'var.my_record'                 # Data must be supplied here
        >>>         'arguments': {
        >>>             'name': 'var.my_record.name',       # 'myname'
        >>>             'keys': ['value']                   # Alternatively: '*' or 'var.my_record.keys()'; includes 'name' field
        >>>         }
        >>>     }
        >>> }

        Use the 'iteration' directive to record many different records using HSET.
        >>> # Represents a variable 'my_records' stored in 'TaskChain.variables'.
        >>> var_my_records = [{'name': 'Bob', 'age': 28, 'eye': 'brown'}, {'name': 'Susan', 'age': 30, 'eye': 'green'}]
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'data': 'var.my_records',       # Data must be supplied here
        >>>         'arguments': {
        >>>             'name': 'item.name',        # [Bob], [Susan]
        >>>             'keys': ['age', 'eye']      # [28, brown], [30, green]
        >>>         },
        >>>         'iteration': 'var.my_records'
        >>>     }
        >>> }
        """

        results = {
            'added': 0,
            'errors': 0,
            'updated': 0
        }

        name = self.arguments.get('name')
        keys = self.arguments.get('keys')
        value = self.arguments.get('value')

        def record_response_code(r: int):
            """
            Records the response code from the Redis operation.
            """
            if r == -1:
                results['errors'] += 1

            elif r == 0:
                results['updated'] += 1

            else:
                results['added'] += 1

        # SET operation
        try:
            if name and value:
                self.calls += 1
                record_response_code(self.silo.connect().set(name=name, value=self.serialize(value), ex=self.expire))

            # HSET operation
            elif name and keys:
                if not self.data:
                    raise TaskException(self, "When 'name' and 'keys' are supplied, the 'data' attribute must be provided.",
                                        "This allows the task to iterate over the keys within the data and store them in "
                                        "the database.")

                # If keys is '*', we treat this as a wildcard operation and iterate over all keys in the data
                if isinstance(keys, str) and keys == '*':
                    keys = list(self.data.keys())

                record_response_code(self.silo.connect().hset(name=name,
                                                          mapping={
                                                              key: self.serialize(self.data[key])
                                                              for key in keys
                                                          })
                                     )

                # If an expiration time is provided, set the expiration time for this record
                if self.expire:
                    self.calls += 1
                    record_response_code(self.silo.connect().expire(name=name, time=self.expire))

            else:
                raise TaskException(self, "Invalid argument combination. Must provide ('name', 'value') or ('name', 'keys').")

        except Exception as ex:
            self.meta['Errors'].append(str(ex))

        return results

    def deserialize(self, v: Any) -> Any:
        """
        Deserializes the value if indicated by the task configuration.

        Arguments:
        v (Any): The value to deserialize.
        """

        if self.serialization and isinstance(v, str):
            from json import loads
            return loads(v)

        else:
            return v

    def serialize(self, v: Any) -> Any:
        """
        Serializes the value if indicated by the task configuration.

        Arguments:
        v (Any): The value to serialize.
        """

        if self.serialization and not isinstance(v, self.VALID_REDIS_TYPES):
            from json import dumps
            return dumps(v, default=str)

        else:
            return v

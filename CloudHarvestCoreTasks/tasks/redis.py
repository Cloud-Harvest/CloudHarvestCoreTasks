from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseDataTask

from logging import getLogger
from typing import Literal

logger = getLogger('harvest')


SERIALIZATION_METHODS = Literal['hset', 'hget', 'serialize', 'deserialize']


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

    def __init__(self, rekey: bool = False, serializer: SERIALIZATION_METHODS = None, serializer_key: str = None, *args, **kwargs):
        """
        Initializes a new instance of the RedisTask class.

        Args:
        expire (int, optional): The expiration time for records in the Redis database. Defaults to None.
        rekey (book, optional): If True, results will be keyed based on the `key` or `keys` argument.
        serializer (SERIALIZATION_METHODS, optional): The serialization method to use. Valid options are 'hset', 'hget',
            'serialize', and 'deserialize'. Defaults to None.
        serializer_key (str, optional): The key to use for serialization. Defaults to None. Required when a serializer
            is indicated in the serializer directive.
        """

        # Initialize the BaseDataTask class
        super().__init__(*args, **kwargs)

        # All redis connection methods are lower case
        self.command = self.command.lower()

        self.rekey = rekey
        self.serializer = serializer
        self.serializer_key = serializer_key

        if self.serializer in ['hset', 'serialize'] and not self.serializer_key:
            raise ValueError('The directive `serializer_key` is required when a `serializer` is indicated in the serializer directive.')

    def method(self) -> 'RedisTask':
        """
        Executes the 'self.command' on the Redis database. Each offered StrictRedis command is prefixed with 'redis_'.
        See the individual methods for more information.

        It was necessary to break each offered command into different methods due to the complexity
        of the Redis api and the need to serialize and deserialize data.
        """

        from json import dumps, loads, JSONDecodeError

        client = self.silo.connect()

        # Serializers of 'hset' or 'serialize' are only available for write operations
        if self.serializer == 'hset':
            # Serialize the data before writing to Redis
            self.arguments[self.serializer_key] = format_hset(self.arguments.get(self.serializer_key) or {})

        elif self.serializer == 'serialize':
            # Serialize the data before writing to Redis
            self.arguments[self.serializer_key] = dumps(self.arguments.get(self.serializer_key) or {}, default=str)

        # Execute the command using the RedisClient
        result = getattr(client, self.command)(**self.arguments)

        # Converts list results back into a dictionary based on the keys provided in self.arguments, but only if the
        # results are a list
        if self.rekey and isinstance(result, list):
            original_keys = self.arguments.get('key') or self.arguments.get('keys') or []
            if isinstance(original_keys, str):
                original_keys = [original_keys]

            result = {
                key: result[original_keys.index(key)]
                for key in original_keys
            }

        if self.serializer == 'hget':
            # Deserialize the data after reading from Redis
            result = unformat_hset(result)

        elif self.serializer == 'deserialize':
            # Deserialize the data after reading from Redis
            try:
                result = loads(result)

            except JSONDecodeError:
                # If the data is not JSON, return it as is
                pass

        self.result = result

        return self


def get_first_scan_match(silo_name: str, pattern: str, count_per_scan: int = 100) -> str or None:
    """
    Returns the first matching record in a Redis database.

    Arguments
    silo_name (str): The name of the silo to connect to.
    pattern (str): The pattern to match.
    count_per_scan (int, optional): The number of records to scan at a time. Default is 100.

    Returns
    str or None: The first matching record, or None if no match is found.
    """

    from CloudHarvestCoreTasks.silos import get_silo
    silo = get_silo(silo_name)

    client = silo.connect()

    cursor = False

    # Get the first task ID
    while cursor != 0:
        cursor, batch = client.scan(cursor=0, match=pattern, count=count_per_scan)

        if batch:
            return batch[0]

    return None


def format_hset(dictionary: dict) -> dict:
    """
    Formats a dictionary to be used with HSET.

    Args:
        dictionary:

    Returns:
        A dictionary formatted for HSET.
    """

    from json import dumps

    result = {}

    if isinstance(dictionary, dict):
        for key, value in dictionary.items():
            if not isinstance(value, (str, int, float)):
                try:
                    result[key] = dumps(value, default=str)

                except TypeError:
                    result[key] = value

            else:
                result[key] = value

    return result

def unformat_hset(dictionary: dict) -> dict:
    """
    Unformats a dictionary to be used with HSET.

    Args:
        dictionary:

    Returns:
        A dictionary formatted for HSET.
    """

    from json import JSONDecodeError, loads
    from copy import deepcopy
    from typing import Any

    dictionary = deepcopy(dictionary)

    def decode_value(value: Any) -> Any:
        if isinstance(value, str):
            try:
                return loads(value)

            except JSONDecodeError:
                # it will often be the case that any arbitrary string is not JSON
                 return value

        elif isinstance(value, (list, tuple)):
            return [decode_value(v) for v in value]

        elif isinstance(value, dict):
            return {k: decode_value(v) for k, v in value.items()}

        else:
            # If the value is not a string, list, tuple, or dict, return it as is
            return value

    return decode_value(dictionary)

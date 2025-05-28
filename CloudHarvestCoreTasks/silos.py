"""
This module contains the Silo class. A silo is a storage location for data which can be retrieved using CloudHarvest.
"""
from CloudHarvestCorePluginManager.decorators import register_definition
from logging import getLogger

logger = getLogger('harvest')

_SILOS = {}


class BaseSilo:
    """
    This class is used to define a Silo. A Silo is a storage location for data which can be retrieved using CloudHarvest.
    Silos are accessible via the CloudHarvestPluginManager and the report syntax where the `silo` directive parameter
    is available.

    Silos manage connection pools to resources and provide heartbeat checks, if necessary.
    """

    SUPPORTED_CLIENT_PARAMETERS = ('host', 'port', 'username', 'password')

    def __init__(self,
                 name: str,
                 host: str,
                 port: int,
                 engine: str,
                 username: str = None,
                 password: str = None,
                 database:str = None,
                 **extended_db_configuration):

        self.name = name
        self.host = host
        self.port = port
        self.engine = engine
        self.username = username
        self.password = password
        self.database = database
        self.extended_db_configuration = extended_db_configuration or {}

        self.pool = None

    def __dict__(self):
        return {
            'host': self.host,
            'port': self.port,
            'engine': self.engine,
            'username': self.username,
            'password': self.password,
            'database': self.database,
            'extended_db_configuration': self.extended_db_configuration
        }

    @property
    def log_prefix(self):
        """
        Returns a string that can be used as a prefix for log messages.
        """

        return f'{self.name}@{self.host}:{self.port}:{self.database}'

    @property
    def is_connected(self) -> bool:
        """
        Checks if the Silo is connected and returns a boolean value.
        """

        raise NotImplementedError

    def add_indexes(self, indexes: dict):
        """
        Adds indexes to the Silo.

        :param indexes: A dictionary of indexes to add to the Silo.
        """
        raise NotImplementedError

    def connect(self):
        """
        Connects to the Silo and returns a connection object.
        """
        raise NotImplementedError

    def call_with_supported_client_parameters(self, func, **kwargs):
        """
        Calls a function with only the supported client parameters.

        :param func: The function to call.
        :param kwargs: The keyword arguments to pass to the function.
        """
        config = {k: v for k, v in kwargs.items() if k in self.SUPPORTED_CLIENT_PARAMETERS}

        return func(**config)

@register_definition('silo', 'mongo')
class MongoSilo(BaseSilo):
    from pymongo import MongoClient

    SUPPORTED_CLIENT_PARAMETERS = BaseSilo.SUPPORTED_CLIENT_PARAMETERS + ('authSource', 'maxPoolSize', 'minPoolSize')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def is_connected(self) -> bool:
        """
        Checks if the MongoDB database is connected and returns a boolean value.

        Returns:
            bool: True if the MongoDB database is connected, False otherwise.
        """

        if self.pool:
            try:
                self.pool.server_info()
                return True

            except BaseException as ex:
                return False

        else:
            return False

    def add_indexes(self, indexes: dict):
        """
        Create an index in the backend cache.

        Args:
            indexes (dict): A dictionary containing the indexes to create.

        Returns:
            None
        """

        # Get the connection
        client = self.connect()

        # Identify collections
        for collection in indexes.keys():

            # Identify indexes
            for index in indexes[collection]:
                position = indexes[collection].index(index)
                try:
                    # Check if the keys are provided
                    if not index.get('keys'):
                        raise ValueError(f'Index keys cannot be empty for collection {collection}.')

                    # Ensure the index is created in the background unless specified
                    index |= {'background': True}  if not index.get('background') else {}

                    # Create the index
                    client[self.database][collection].create_index(**index)

                except BaseException as ex:
                    logger.error(f'Failed to create index {position} in collection {self.database}[{collection}] : {ex}')

                else:
                    logger.debug(f'Created index {position} in collection {self.database}[{collection}].')

    def connect(self) -> MongoClient:
        """
        Connects to the MongoDB database using the provided configuration, returning a MongoClient object. If the client
        already exists, it will return the existing client object.

        Returns:
            MongoClient: A MongoClient instance connected to the specified database.
        """

        # Already connected
        if self.is_connected:
            return self.pool

        # Prepare the configuration
        default_configuration = {
            'maxPoolSize': 50,
        }

        config = default_configuration | self.__dict__() | self.extended_db_configuration

        # Create the client object
        from pymongo import MongoClient
        self.pool: MongoClient = self.call_with_supported_client_parameters(MongoClient, **config)

        # Test that the connection works
        if self.is_connected:
            return self.pool

        else:
            raise ConnectionError(f'Could not connect to the MongoDB database {self.name}.')

@register_definition('silo', 'redis')
class RedisSilo(BaseSilo):
    from redis import StrictRedis

    SUPPORTED_CLIENT_PARAMETERS = BaseSilo.SUPPORTED_CLIENT_PARAMETERS + ('db', 'max_connections', 'decode_responses')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def is_connected(self) -> bool:
        """
        Checks if the Redis cache is connected and returns a boolean value.

        Returns:
            bool: True if the Redis cache is connected, False otherwise.
        """

        if self.pool:
            try:
                from redis import StrictRedis
                StrictRedis(connection_pool=self.pool).ping()
                return True

            except BaseException:
                return False

        else:
            return False

    def connect(self) -> StrictRedis:
        """
        Connects to the Redis cache using the provided configuration, returning a StrictRedis object. If the client
        already exists, it will return the existing client object.

        Returns:
            StrictRedis: A StrictRedis instance connected to the specified database.
        """

        # Check if a connection pool already exists for the specified database
        if self.is_connected:
            from redis import StrictRedis
            return StrictRedis(connection_pool=self.pool)

        # Create a new connection pool for the specified database
        default_configuration = {
            'db': self.database,
            'max_connections': 50,
            'decode_responses': True
        }

        config = default_configuration | self.__dict__()

        from redis import ConnectionPool
        self.pool: ConnectionPool = self.call_with_supported_client_parameters(ConnectionPool, **config)

        from redis import StrictRedis
        connection = StrictRedis(connection_pool=self.pool)

        return connection


def add_silo(name: str, **kwargs) -> BaseSilo:
    """
    Add a silo to the silo registry.

    Arguments
    name (str): The name of the silo.
    """

    engine = kwargs['engine']

    # We use the Registry here in case there are plugins which support silo engines not defined in the CoreTasks repo.
    from CloudHarvestCorePluginManager.registry import Registry

    # Retrieve the class name
    silo_class = Registry.find(result_key='cls', name=engine, category='silo')

    # Raise an error if the class is not found
    if not silo_class:
        raise NotImplementedError(f'Unknown silo engine: {engine} for silo {name}')

    # Create the silo object
    silo = silo_class[0](name=name, **kwargs)

    silo.connect()
    if silo.is_connected:
        # Add the silo to the top-level _SILOS dictionary
        _SILOS[name] = silo

    else:
        raise ConnectionError(f'Could not connect to the silo {name}.')

    return silo


def clear_silos():
    """
    Clear all silos from the silo registry.
    """
    _SILOS.clear()


def drop_silo(name: str):
    """
    Remove a silo from the silo registry.

    :param name: The name of the silo to remove.
    """
    if name in _SILOS:
        _SILOS.pop(name)

def drop_silos(*names):
    """
    Remove multiple silos from the silo registry.

    :param names: A list of silo names to remove.
    """
    for name in names:
        drop_silo(name)


def get_silo(name: str) -> BaseSilo:
    """
    Retrieve a silo by name.

    :param name: The name of the silo to retrieve.
    :return: The silo object.
    """
    return _SILOS.get(name)

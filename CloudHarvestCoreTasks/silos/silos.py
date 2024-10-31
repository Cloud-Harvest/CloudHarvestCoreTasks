"""
This module contains the Silo class. A silo is a storage location for data which can be retrieved using CloudHarvest.
"""
from redis import StrictRedis

_SILOS = {}


class BaseSilo:
    """
    This class is used to define a Silo. A Silo is a storage location for data which can be retrieved using CloudHarvest.
    Silos are accessible via the CloudHarvestPluginManager and the report syntax where the `silo` directive parameter
    is available.

    Silos manage connection pools to resources and provide heartbeat checks, if necessary.
    """

    def __init__(self,
                 name: str,
                 host: str,
                 port: int,
                 engine: str,
                 username: str = None,
                 password: str = None,
                 database:str = None,
                 heartbeat: dict = None,
                 **additional_database_arguments):

        self.name = name
        self.host = host
        self.port = port
        self.engine = engine
        self.username = username
        self.password = password
        self.database = database
        self.additional_database_arguments = additional_database_arguments or {}

        self.pool = None
        self.heartbeat = None
        self.heartbeat_thread = None

    def __dict__(self):
        return {
            'name': self.name,
            'host': self.host,
            'port': self.port,
            'engine': self.engine,
            'username': self.username,
            'password': self.password,
            'database': self.database
        } | self.additional_database_arguments

    @property
    def is_connected(self) -> bool:
        """
        Checks if the Silo is connected and returns a boolean value.
        """

        raise NotImplementedError

    def connect(self):
        """
        Connects to the Silo and returns a connection object.
        """
        raise NotImplementedError


class MongoBaseSilo(BaseSilo):
    from pymongo import MongoClient

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def is_connected(self) -> bool:
        """
        Checks if the MongoDB database is connected and returns a boolean value.

        Args:
            database (str): The name of the database to check the connection for.

        Returns:
            bool: True if the MongoDB database is connected, False otherwise.
        """



        if self.pool:
            try:
                self.pool.server_info()
                return True

            except Exception:
                return False

        else:
            return False

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

        config = default_configuration | self.__dict__()

        # Create the client object
        from pymongo import MongoClient
        self.pool = MongoClient(**config)

        # Test that the connection works
        if self.is_connected:
            return self.pool

        else:
            raise ConnectionError(f'Could not connect to the MongoDB database {self.name}.')


class RedisBaseSilo(BaseSilo):
    from redis import StrictRedis

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
                StrictRedis(connection_pool=self.pool).ping()
                return True

            except Exception:
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
            return StrictRedis(connection_pool=self.pool)

        # Create a new connection pool for the specified database
        default_configuration = {
            'db': self.database,
            'max_connections': 50,
            'decode_responses': True
        }

        config = default_configuration | self.__dict__()

        from redis import ConnectionPool
        self.pool = ConnectionPool(**config)

        connection = StrictRedis(connection_pool=self.pool)

        return connection


def add_silo(name: str, engine: str, **kwargs) -> BaseSilo:
    """
    Add a silo to the silo registry.

    Arguments
    name (str): The name of the silo.
    engine (str): The engine to use for the silo.

    """

    match kwargs.get('engine'):
        case 'mongo':
            silo = MongoBaseSilo(**kwargs)

        case _:
            raise NotImplementedError(f'Unknown silo engine: {engine} for silo {name}')

    _SILOS[name] = silo

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


def get_silo(name: str) -> BaseSilo:
    """
    Retrieve a silo by name.

    :param name: The name of the silo to retrieve.
    :return: The silo object.
    """
    return _SILOS.get(name)


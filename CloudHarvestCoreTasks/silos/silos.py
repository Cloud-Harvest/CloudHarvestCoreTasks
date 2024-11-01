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

    def __init__(self,
                 name: str,
                 host: str,
                 port: int,
                 engine: str,
                 username: str = None,
                 password: str = None,
                 database:str = None,
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

    def connect(self):
        """
        Connects to the Silo and returns a connection object.
        """
        raise NotImplementedError

@register_definition('silo', 'mongo_silo')
class MongoSilo(BaseSilo):
    from pymongo import MongoClient

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

@register_definition('silo', 'redis_silo')
class RedisSilo(BaseSilo):
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
                from redis import StrictRedis
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
        self.pool = ConnectionPool(**config)

        from redis import StrictRedis
        connection = StrictRedis(connection_pool=self.pool)

        return connection


def add_silo(name: str, engine: str, **kwargs) -> BaseSilo:
    """
    Add a silo to the silo registry.

    Arguments
    name (str): The name of the silo.
    engine (str): The engine to use for the silo.
    """

    # We use the Registry here in case there are plugins which support silo engines not defined in the CoreTasks repo.
    from CloudHarvestCorePluginManager.registry import Registry

    # Define the class name
    class_name = f'{engine.title()}_silo'

    # Retrieve the class name
    silo_class = Registry.find(result_key='cls', name=class_name, category='silo')

    # Raise an error if the class is not found
    if not silo_class:
        raise NotImplementedError(f'Unknown silo engine: {engine} for silo {name}')

    # Create the silo object
    silo = silo_class[0](**kwargs)

    # Add the silo to the top-level _SILOS dictionary
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


from threading import Thread
from typing import Literal
def start_heartbeat_on_silo(name: str, node_type: Literal['agent', 'api'], version: str, heartbeat_check_rate: float = 1.0) -> Thread:
    """
    Start the heartbeat process on the specified silo.

    Arguments:
    name (str): The name of the silo.
    version (str): The version of the application.
    heartbeat_check_rate (float): The rate at which the heartbeat process should check the node status.
    """

    # Start the api heartbeat on the appropriate silo:
    from CloudHarvestCoreTasks.silos import get_silo
    silo = get_silo(name)

    from threading import Thread
    def heartbeat():
        """
        The function that runs in the background thread to update the node status in the Redis cache.
        If the heartbeat process encounters an error, it will log the error and continue running.
        """

        import platform
        from socket import getfqdn, gethostbyname
        from datetime import datetime, timezone

        # Record the start time of the heartbeat process
        start_datetime = datetime.now(tz=timezone.utc)

        from os.path import exists
        plugins_txt = 'app/plugins.txt'
        plugins = []
        if exists(plugins_txt):
            with open(plugins_txt) as plugins_txt_stream:
                plugins = plugins_txt_stream.readlines()

        # Gather node information
        node_info = {
            "type": node_type,
            "hostname": getfqdn(),
            "ip": gethostbyname(getfqdn()),
            "os": platform.system(),
            "plugins": plugins,
            "version": version,
            "start": start_datetime.isoformat(),
        }

        while True:
            from time import sleep
            node_info['last'] = datetime.now(tz=timezone.utc).isoformat()

            try:
                silo.connect().hset(name=f'{node_type}::{node_info["hostname"]}', mapping=node_info)
                logger.debug(f'heartbeat: OK: {node_info}')

            except Exception as e:
                logger.error(f'heartbeat: Could not update the node status in the Redis cache: {e.args}')

            sleep(heartbeat_check_rate)

    return Thread(target=heartbeat, daemon=True)
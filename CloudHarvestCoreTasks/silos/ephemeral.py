"""
The Ephemeral Silo is a cache that is stored in a non-persistent storage. For Harvest, this means the Redis backend.
We call this a cache because we expect the data to be invalidated at some point in the future (ie job queues, tokens,
and agent/api node heartbeats).

We accept that this cache is ephemeral and that data stored in it may be lost at any time. In the event of a loss of the
cache, the system should be able to recover gracefully and continue to operate as expected once the cache layer as
been restarted.

At the time of writing, we have no plans to cache query results from our persistent storage in the Ephemeral Silo. This
is because we anticipate a combination of query tuning, indexing, configuration, and caching at the persistent layer to
be sufficient for our needs. We may revisit this decision in the future if we find that we need to cache query results
in the Ephemeral Silo.
"""
from logging import getLogger
from redis import StrictRedis, ConnectionPool
from typing import Literal

logger = getLogger('harvest')


_CLIENTS = {}

def connect(database: str, *args, **kwargs) -> StrictRedis:
    """
    Connects to the Redis cache using the provided configuration, returning a StrictRedis object. If the client
    already exists, it will return the existing client object.

    Args:
        database (str): The name of the database to connect to.
        *args: Additional positional arguments for the ConnectionPool.
        **kwargs: Additional keyword arguments for the ConnectionPool.

    Returns:
        StrictRedis: A StrictRedis instance connected to the specified database.
    """

    if _CLIENTS.get(database):
        return _CLIENTS.get(database)

    default_configuration = {
        'db': database,
        'max_connections': 50,
    }

    _pool = ConnectionPool(*args, **default_configuration | kwargs)
    _CLIENTS[database] = _pool

    return StrictRedis(connection_pool=_CLIENTS[database])

def is_connected(database: str) -> bool:
    """
    Checks if the Redis cache is connected and returns a boolean value.

    Args:
        database (str): The name of the database to check the connection for.

    Returns:
        bool: True if the Redis cache is connected, False otherwise.
    """

    if _CLIENTS.get(database):
        client = connect(database)

        try:
            client.ping()
            return True

        except Exception:
            return False

    else:
        return False


def start_heartbeat(heartbeat_type: Literal['agent', 'api'], database: str = 'harvest', check_rate: float = 1.0):
    """
    Starts a heartbeat process that periodically updates the status of the node in the Redis cache.

    Parameters:
    database (str): The name of the database to connect to.

    Returns:
    Thread: The thread running the heartbeat process.
    """

    # Connect to the Redis cache using the EphemeralCache class
    client = connect(database)

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

    from json import load
    with open('./meta.json') as config_stream:
        config = load(config_stream)

    # Gather node information
    node_info = {
        "hostname": getfqdn(),
        "ip": gethostbyname(getfqdn()),
        "os": platform.system(),
        "plugins": plugins,
        "version": config.get('version'),
        "start": start_datetime.isoformat(),
    }

    def _run():
        """
        The function that runs in the background thread to update the node status in the Redis cache. If the heartbeat
        process encounters an error, it will log the error and continue running. In the event the process cannot update
        its status in the cache, the key will expire and the node will be considered offline.
        """
        while True:
            message = 'OK'
            level = 'debug'

            try:
                # Update the last update time
                node_info['last'] = datetime.now(tz=timezone.utc).isoformat()

                # Update the node status in the Redis cache
                key = f"harvest:heartbeat_{heartbeat_type}:{getfqdn()}"
                client.hset(key, mapping=node_info)

                # Set the expiration time for the key
                client.expire(key, int(2 * check_rate))

            except Exception as ex:
                message = ' '.join(ex.args)
                level = 'error'

            finally:
                # Log the heartbeat status
                getattr(logger, level)(f'heartbeat: {message}')
                from time import sleep
                sleep(check_rate)

    # Create and start the background thread for the heartbeat process
    from threading import Thread
    thread = Thread(target=_run, args=(client,), name='heartbeat', daemon=True)
    thread.start()

    return thread

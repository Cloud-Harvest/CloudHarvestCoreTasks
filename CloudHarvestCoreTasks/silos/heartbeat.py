"""
The heartbeat module contains the Heartbeat class, which is used to manage the heartbeat process for a node in the CloudHarvest system.
"""

from logging import getLogger
logger = getLogger('harvest')


class Heartbeat:
    """
    The Heartbeat class is used to manage the heartbeat process for a node in the CloudHarvest system. The heartbeat
    """
    from threading import Thread
    from typing import Literal

    def __init__(self, silo_name: str, node_type: Literal['agent', 'api'], version: str, heartbeat_check_rate: float = 1.0):
        """
        Initialize the Heartbeat object.

        Arguments:
        name (str): The name of the silo.
        version (str): The version of the application.
        heartbeat_check_rate (float): The rate at which the heartbeat process should check the node status.
        """

        self.silo_name = silo_name
        self.node_type = node_type
        self.version = version
        self.heartbeat_check_rate = heartbeat_check_rate

        self.thread = None
        self.status: str = 'initialized'


    def start(self):
        """
        Start the heartbeat process on the specified silo.

        Arguments:
        name (str): The name of the silo.
        version (str): The version of the application.
        heartbeat_check_rate (float): The rate at which the heartbeat process should check the node status.
        """

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
                "type": self.node_type,
                "hostname": getfqdn(),
                "ip": gethostbyname(getfqdn()),
                "os": platform.system(),
                "plugins": plugins,
                "version": self.version,
                "start": start_datetime.isoformat(),
            }

            from CloudHarvestCoreTasks.silos import get_silo
            silo = get_silo(self.silo_name)

            self.status = 'running'

            while self.status == 'running':
                from time import sleep
                node_info['last'] = datetime.now(tz=timezone.utc).isoformat()

                try:
                    silo.connect().hset(name=f'{self.node_type}::{node_info["hostname"]}', mapping=node_info)
                    logger.debug(f'heartbeat: OK: {node_info}')

                except Exception as e:
                    logger.error(f'heartbeat: Could not update the node status in the Redis cache: {e.args}')

                sleep(self.heartbeat_check_rate)

        self.thread = Thread(target=heartbeat, daemon=True)

        return self

    def stop(self):
        """
        Stop the heartbeat process.
        """

        self.status = 'stopped'
        self.thread.join()

        return self

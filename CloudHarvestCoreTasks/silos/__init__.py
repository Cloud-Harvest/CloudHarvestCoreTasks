"""
Silos are data sources which can be accessed by CloudHarvest. They are defined in the `silo` directive of a report and
are used to access data for the report. This module provides the ability to add, drop, and retrieve silos from the
CloudHarvest Silo registry.
"""

from .silos import (
    add_silo,
    clear_silos,
    drop_silo,
    get_silo,
    start_heartbeat_on_silo,
    BaseSilo,
    MongoSilo,
    RedisSilo
)
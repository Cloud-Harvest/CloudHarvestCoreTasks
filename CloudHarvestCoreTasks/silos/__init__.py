"""
This module is responsible for managing the silos that are used to store the data that is harvested from the cloud.
"""

from .silos import (
    Silo,
    add_silo,
    get_one_silo,
    get_many_silos,
    get_silo_like,
    list_silos,
    remove_silos
)
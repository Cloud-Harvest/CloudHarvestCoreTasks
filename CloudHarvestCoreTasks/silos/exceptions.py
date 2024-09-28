"""
This module contains the exceptions that are raised by the silos module.
"""

from tasks.base import BaseHarvestException


class BaseCacheException(BaseHarvestException):
    """
    This exception is raised when an error occurs in the silos module.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class EphemerisCacheException(BaseCacheException):
    """
    This exception is raised when an error occurs in the Ephemeral Silo.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class PersistentCacheException(BaseCacheException):
    """
    This exception is raised when an error occurs in the persistent silo.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

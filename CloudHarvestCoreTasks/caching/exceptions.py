"""
This module contains the exceptions that are raised by the caching module.
"""

from base import BaseHarvestException


class BaseCacheException(BaseHarvestException):
    """
    This exception is raised when an error occurs in the caching module.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class EphemerisCacheException(BaseCacheException):
    """
    This exception is raised when an error occurs in the ephemeral cache.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class PersistentCacheException(BaseCacheException):
    """
    This exception is raised when an error occurs in the persistent cache.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

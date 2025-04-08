from typing import Any


class CachedData:
    def __init__(self, data: Any, valid_age: float = 0):
        """
        A simple object which retains data and a timestamp of when it was recorded.

        Arguments
        data (Any): The data to store.
        valid_age (float): The age in seconds before the data is considered invalid. The default is 0 seconds
        (immediate expiration) which promotes the use of update() to set a valid age when the data is recorded.
        """

        from datetime import datetime
        self._data = data
        self._valid_age = valid_age
        self.recorded = datetime.now()

    @property
    def age(self) -> float:
        """
        Returns the age of the CachedData in seconds.
        """

        from datetime import datetime
        return (datetime.now() - self.recorded).total_seconds()

    @property
    def data(self) -> Any:
        """
        Returns the data stored in CachedData.
        """

        return self._data

    @property
    def is_valid(self) -> bool:
        """
        Returns whether the CachedData is still valid.
        """

        return self.age < self._valid_age

    @property
    def valid_age(self) -> float:
        """
        Returns the valid age of the CachedData in seconds.
        """

        return self._valid_age

    def update(self, data: Any, valid_age: float = None) -> 'CachedData':
        """
        Updates the data stored in CachedData.

        Arguments
        data (Any): The new data to store.
        valid_age (float): The age in seconds before the data is considered invalid. Default is valid_age assigned at initialization.

        Returns
        The CachedData object.
        """

        from datetime import datetime
        self._data = data
        self._valid_age = valid_age or self._valid_age
        self.recorded = datetime.now()

        return self

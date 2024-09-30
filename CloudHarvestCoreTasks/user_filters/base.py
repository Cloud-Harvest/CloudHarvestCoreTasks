"""
This module contains the BaseUserFilter class which is used to define the structure of the user filters. UserFilters are
typically provided by a user via a client, such as the CloudHarvestCLI, but could be supplied directly by a developer
via Python or a Task configuration file.
"""
from typing import List


class BaseUserFilter:
    """
    Base class for user filters. This class is used to define the structure of the user filters. It is not intended
    for direct use; rather, it is intended to be inherited by the specific user filter classes.
    """
    def __init__(self,
                 accepted: str = None,
                 add_keys: List[str] = None,
                 count: str = None,
                 exclude_keys: List[str] = None,
                 headers: List[str] = None,
                 limit: int = None,
                 matches: List[List[str]] = None,
                 sort: List[str] = None,
                 *arg, **kwargs):

        """
        Arguments:
            accepted (str, optional): A string containing the filter types to be applied.
                                                The default is '' which applies no filters.
            add_keys (List[str], optional): The keys to be added to the data. Defaults to an empty list.
            count (str, optional): The count of the data. Defaults to None.
            exclude_keys (List[str], optional): The keys to be excluded from the data. Defaults to an empty list.
            headers (List[str], optional): The headers of the data. Defaults to an empty list.
            limit (int, optional): The limit of the data. Defaults to None.
            matches (List[List[str]], optional): The matches of the data. Defaults to an empty list.
            sort (List[str], optional): The sort of the data. Defaults to an empty list.
        """

        from re import compile
        self.accepted = compile(accepted) if isinstance(accepted, str) else accepted

        self.add_keys = add_keys or []
        self.count = count
        self.exclude_keys = exclude_keys or []
        self.headers = headers or []
        self.limit = limit
        self.matches = matches or []
        self.sort = sort or []

        self.pre_syntax = None
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _headers(self):
        """
        This method returns the headers of the data based on the provided headers, add_keys, and exclude_keys.
        """
        headers = self.headers if self.accepted.match('headers') else []
        add_keys = self.add_keys if self.accepted.match('add_keys') else []
        exclude_keys = self.exclude_keys if self.accepted.match('exclude_keys') else []

        for header in headers + add_keys:
            if header not in headers and header not in exclude_keys:
                headers.append(header)

        return headers

    def _sort(self, headers: List[str]) -> dict:
        """
        This method returns the sort of the data based on the provided sort.
        """
        sort = self.sort if self.accepted.match('sort') else []

        sorted_keys = {}
        for s in sort:
            if ':' in s:
                key, value = s.split(':')
                key = key.strip()

                # Skip keys not in the headers as they cannot be included in the sort
                if key not in headers:
                    continue

                # Check if this key needs to be sorted in ascending or descending order
                if value.lower() == 'desc':
                    order = -1
                else:
                    # Ascending order is the default when 'desc' is not provided
                    order = 1

            # If no order is provided, default to ascending order
            else:
                key = s
                order = 1

            sorted_keys[key] = order

        return sorted_keys

    def apply(self) -> 'BaseUserFilter':
        """
        This method gathers the user filters and returns the derived syntax for various data destinations.

        Arguments:
            filter_types {str} -- A string containing the filter types to be applied. The default is '*' which applies
            all filters.
        """

        headers = self.headers if self.accepted.match('headers') else self._headers()
        sort = self._sort(headers)

        self.pre_syntax = {
            'add_keys': self.add_keys if self.accepted.match('add_keys') else None,
            'count': self.count if self.accepted.match('count') else None,
            'exclude_keys': self.exclude_keys if self.accepted.match('exclude_keys') else None,
            'headers': headers,
            'limit': self.limit if self.accepted.match('limit') else None,
            'matches': self.matches if self.accepted.match('matches') else None,
            'sort': sort if self.accepted.match('sort') else None
        }

        return self

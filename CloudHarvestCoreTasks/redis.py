"""
Redis helper functions for HSET and HGET
"""

from json import JSONDecodeError, dumps, loads
from logging import getLogger

logger = getLogger('harvest')


def format_hset(dictionary: dict) -> dict:
    """
    Formats a dictionary to be used with HSET.

    Args:
        dictionary:

    Returns:
        A dictionary formatted for HSET.
    """

    from json import dumps

    if isinstance(dictionary, dict):
        for key, value in dictionary.items():
            if not isinstance(value, (str, int, float)):
                try:
                    dictionary[key] = dumps(value, default=str)

                except TypeError:
                    logger.error(f'Failed to format the value of key `{key}` for HSET')

    return dictionary

def unformat_hset(dictionary: dict) -> dict:
    """
    Unformats a dictionary to be used with HSET.

    Args:
        dictionary:

    Returns:
        A dictionary formatted for HSET.
    """

    from json import loads

    if isinstance(dictionary, dict):
        for key, value in dictionary.items():
            if isinstance(value, str):
                try:
                    dictionary[key] = loads(value)

                except JSONDecodeError:
                    # it will often be the case that any arbitrary string is not JSON
                    pass

    return dictionary

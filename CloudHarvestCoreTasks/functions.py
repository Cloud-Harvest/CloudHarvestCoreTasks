"""
This module contains various data manipulation functions for casting, converting, and transforming data. It is intended
to be used in conjunction with the HarvestRecord and HarvestRecordSet classes to manipulate data in record sets.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal


CAST_TYPES = Literal['bool', 'str', 'int', 'float', 'list', 'dict', 'datetime.fromtimestamp', 'datetime.fromisoformat']

def cast(value: Any, typeof: CAST_TYPES or str) -> (bool, str, int, float, list, dict, datetime):
    """
    Converts a value into a specific type based on a parameter which is a string representation of the desired type.

    Parameters:
    value (Any): The value to be converted.
    typeof (Literal['bool', 'str', 'int', 'float', 'list', 'dict', 'datetime.fromtimestamp', 'datetime.fromisoformat']): The string representation of the target type.

    Returns:
    Union[bool, str, int, float, list, dict, datetime]: The converted value or None if the conversion fails or if the target type is not supported.
    """

    type_mapping = {
        'int': int,
        'float': float,
        'str': str,
        'bool': bool,
        'list': list,
        'dict': dict,
        'datetime.fromtimestamp': datetime.fromtimestamp,
        'datetime.fromisoformat': datetime.fromisoformat
    }

    if typeof in type_mapping:
        try:
            if typeof == 'bool':
                if value in (False, None, 'False', 'false', 'No', 'no'):
                    result = False
                else:
                    result = True
            else:
                result = type_mapping[typeof](value)

            return result

        except ValueError:
            return None

    else:
        return None


def fuzzy_cast(value: Any) -> Any:
    """
    Attempts to cast a value to a more appropriate type based on the value itself.
    Args:
        value: The input variable to cast.

    Returns: Any
    """

    # Check if the value is a boolean
    if is_bool(value):
        cast_variables_as = 'bool'

    # Check if the value is a datetime
    elif is_datetime(value):
        cast_variables_as = 'datetime'

    # Check if the value is null
    elif is_null(value):
        cast_variables_as = 'null'

    # Check if the value is a number
    elif is_number(value):
        # If the value is a string and contains a decimal point, cast it as a float
        if isinstance(value, str):
            if '.' in str(value):
                cast_variables_as = 'float'
            else:
                # If the value is a string and does not contain a decimal point, cast it as an integer
                cast_variables_as = 'int'
        else:
            # If the value is not a string, cast it as a float
            cast_variables_as = 'float'

    # If none of the above conditions are met, cast the value as a string
    else:
        cast_variables_as = 'str'

    # Use the cast() function to cast the value to the determined type
    return cast(value, cast_variables_as)


def delimiter_list_to_string(value: list, delimiter: str) -> str:
    """
    Splits a string into a list based on a delimiter. This is especially useful when changing a rich Table output to
    span multiple lines using '\n'.
    :param value: A list to join
    :param delimiter: The delimiter to use when joining the list
    :return: A delimited string
    """
    return delimiter.join(value)


def is_bool(value: str) -> bool:
    """
    Determines if a value is a boolean.
    :param value: The value to check.
    :return: A boolean indicating if the value is a boolean.
    """

    if value in ('False', 'false', 'No', 'no', 'True', 'true', 'Yes', 'yes'):
        return True


def is_datetime(value: str) -> bool:
    """
    Determines if a value is a datetime.
    :param value: The value to check.
    :return: A boolean indicating if the value is a datetime.
    """
    try:
        datetime.fromisoformat(value)
        return True

    except ValueError:
        return False


def is_null(value: str) -> bool:
    """
    Determines if a value is null.
    :param value: The value to check.
    :return: A boolean indicating if the value is null.
    """
    if value in (None, 'None', 'null'):
        return True


def is_number(value: str) -> bool:
    """
    Determines if a value is a number.
    :param value: The value to check.
    :return: A boolean indicating if the value is a number.
    """
    try:
        int(value)
        return True

    except ValueError:
        return False


def key_value_list_to_dict(value: List[Dict], key_name: str = 'Key', value_name: str = 'Value') -> dict:
    """
    Converts a list of dictionaries to a dictionary of key value pairs. A common example of this usage is converting
    AWS Tags lists of [{Key: 'Name', Value: 'MyName'}] to {'Name': 'MyName'}.
    :param value: List of dictionaries
    :param key_name: The key name to use for the key in the dictionary
    :param value_name: The key name to use for the value in the dictionary
    :return: A dictionary of key value pairs
    """
    return {
        item[key_name]: item.get(value_name)
        for item in value if key_name in item.keys()
    }


if __name__ == '__main__':
    assert cast(1, 'str') == '1'
    assert cast(1.3, 'int') == 1
    assert cast('1', 'int') == 1
    assert cast(1, 'float') == 1.0
    assert cast('False', 'bool') is False
    assert cast('No', 'bool') is False
    assert cast('No', 'bool') is False
    assert cast('Yes', 'bool') is True


def get_nested_values(s: str, d: dict):
    """
    This function takes a string `s` and a dictionary `d` as inputs. The string `s` represents a sequence of keys
    separated by periods, and the dictionary `d` is a nested structure of dictionaries and lists. The function walks
    through the dictionary `d` following the sequence of keys in `s`, and returns a list of all values that match the
    key path specified by `s`. If `s` specifies a path that includes a list, the function will return values from all
    items in the list.

    This function was developed with the intention of addressing the following use case:
        - EC2 describe_db_instances returns a Reservations (list) with Groups (list) and Instances (list)
        - For the purposes of retreiving just Instances of Groups, the function can be used to extract either key

        >>> {
        >>>     "Reservations": [
        >>>         {
        >>>             "Groups": [
        >>>                 {
        >>>                     "GroupName": "string",
        >>>                     "GroupId": "string"
        >>>                 }
        >>>             ],
        >>>             "Instances": [
        >>>                 {
        >>>                     "AmiLaunchIndex": 123,
        >>>                     "ImageId": "string",
        >>>                     "InstanceId": "string"
        >>>                 }
        >>>             ]
        >>>         }
        >>>     ],
        >>>     "NextToken": "string"
        >>> }

    Args:
        s (str): A string representing a sequence of keys separated by periods.
        d (dict): A dictionary with a nested structure of dictionaries and lists.

    Returns:
        list: A list of all values that match the key path specified by `s`.
    """

    # Split the input string `s` by periods to get a list of keys
    keys = s.split('.')

    # Initialize an empty list `results` to store the final results
    results = []

    def walk_dict(d, keys):
        """
        This is a helper function that walks through the dictionary `d` following the sequence of keys.

        Args:
            d (dict or list): A dictionary or list to walk through.
            keys (list): A list of keys to follow.
        """

        # If `keys` is empty, append `d` to `results`
        if not keys:
            if isinstance(d, list):
                results.extend(d)

            else:
                results.append(d)

        else:
            # Get the first key and the rest of the keys
            first_key, rest_keys = keys[0], keys[1:]

            # If `d` is a dictionary and the first key is in `d`, call `walk_dict` with `d[first_key]` and the rest of the keys
            if isinstance(d, dict) and first_key in d:
                walk_dict(d[first_key], rest_keys)

            # If `d` is a list, iterate over its elements. For each element, call `walk_dict` with the element and `keys`
            elif isinstance(d, list):
                for item in d:
                    walk_dict(item, keys)

    # Call `walk_dict` with the input dictionary `d` and the list of keys
    walk_dict(d, keys)

    # Return `results`
    return results

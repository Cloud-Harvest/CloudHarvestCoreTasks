"""
This module contains helper functions that are used in the CloudHarvestCoreTasks module.
"""

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

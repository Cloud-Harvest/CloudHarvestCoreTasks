from typing import Any, List, Literal

# VARIABLES-------------------------------------------------------------------------------------------------------------
# The following operations are supported by the perform_operation() method:
VALID_MATHS_OPERATIONS = Literal['add', 'subtract', 'multiply', 'divide', 'average', 'minimum', 'maximum']

# DECORATORS------------------------------------------------------------------------------------------------------------
def requires_flatten(method, preserve_lists: bool = False):
    """
    Decorator to flatten and unflatten the data set before and after a method is called.

    Arguments
    method (function): The method to decorate.
    preserve_lists (bool, optional): When true, lists will not be flattened. Defaults to False.
    """

    def wrapper(self, *args, **kwargs):
        # Flatten the data set
        self.flatten(preserve_lists=preserve_lists)

        # Call the method
        result = method(self, *args, **kwargs)

        # Unflatten the data set
        self.unflatten()

        return result

    return wrapper

# CLASSES---------------------------------------------------------------------------------------------------------------
class WalkableDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def assign(self, key: str, value: Any, separator: str = '.') -> dict:
        """
        Assigns a value to a key in a self.

        Arguments
        key (str): The key to assign the value to.
        value (Any): The value to assign.
        separator (str, optional): The separator to use when splitting the key. Defaults to '.'.
        """

        # If the key does not contain the separator, bypass the walking logic and return the value directly
        # This is a performance optimization for top-level keys
        if separator not in key:
            self[key] = value
            return self

        # Split the key into individual path using the separator
        path = key.split(separator)
        target = self

        # Iterate through each key in the path except the last one
        for part in path[:-1]:
            if isinstance(target, dict):
                # Get the next nested dictionary
                target = target.get(part)

            elif isinstance(target, list) and part.isdigit():
                # If the current target is a list and the part is an integer, treat it as an index
                index = int(part)

                if 0 <= index < len(target):
                    target = target[index]

                else:
                    # If the index is out of bounds, return the original self
                    return self

            else:
                # If the path cannot be resolved, return the original self
                return self

        # Assign the value to the last key in the path
        last_key = path[-1]
        if isinstance(target, dict):
            target[last_key] = value

        elif isinstance(target, list) and last_key.isdigit():
            index = int(last_key)

            if 0 <= index < len(target):
                target[index] = value

        return self

    def drop(self, key: str, default: Any = None, separator: str = '.') -> Any:
        """
        Drops a key or index item from self. If the item does not exist, the default value is returned.

        Arguments
        key (str): The key to drop.
        default (Any, optional): The default value to return if the key does not exist. Defaults to None.
        separator (str, optional): The separator to use when splitting the key. Defaults to '.'.
        """
        # If the key does not contain the separator, bypass the walking logic and return the value directly
        # This is a performance optimization for top-level keys
        if separator not in key:
            return self.pop(key, default)

        # Split the key into individual path using the separator
        path = key.split(separator)
        target = self

        # Iterate through each key in the path except the last one
        for part in path[:-1]:
            if isinstance(target, dict):
                # Get the next nested dictionary
                target = target.get(part)

            elif isinstance(target, list) and part.isdigit():
                # If the current target is a list and the part is an integer, treat it as an index
                index = int(part)

                if 0 <= index < len(target):
                    target = target[index]

                else:
                    # If the index is out of bounds, return the original self
                    return self

            else:
                # If the path cannot be resolved, return the original self
                return self

        # Drop the item from the last key in the path
        last_key = path[-1]
        result = default
        if isinstance(target, dict):
            result = target.pop(last_key, None)

        elif isinstance(target, list) and last_key.isdigit():
            index = int(last_key)

            if 0 <= index < len(target):
                result = target.pop(index)

        return result

    def walk(self, key: str, default: Any = None, separator: str = '.') -> Any:
        """
        Walks the self to get the value of a key.

        Arguments
        key (str): The key to get the value from.
        default (Any, optional): The default value to return if the key does not exist. Defaults to None.
        separator (str, optional): The separator to use when walking the self. Defaults to '.'.
        """

        # If the key does not contain the separator, bypass the walking logic and return the value directly
        # This is a performance optimization for top-level keys
        if separator not in key:
            return self.get(key) or default

        # Split the key into individual path using the separator
        path = key.split(separator)
        result = default

        # Start with the base self object as we walk the path to the target value
        target = self

        for part in path:
            try:
                target = target[part]

            except KeyError or IndexError or TypeError:
                result = default
                break

            else:
                result = target

        return result


class DataSet(List[WalkableDict]):
    from functions import CAST_TYPES
    from filters import MatchSetGroup

    def __init__(self, *args):
        super().__init__()

        if args:
            self.add_records(args)

        self.maths_results = WalkableDict()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()
        return None

    def key_values(self, source_key: str, missing_value: Any = None):
        """
        Consolidates the values of a key in the data set.

        Arguments
        source_key (str): The key to consolidate.
        missing_value (Any): The value to use when a key is missing.
        """

        for record in self:
            yield record.walk(source_key, missing_value)

    @property
    def keys(self):
        """
        Returns a generator of all keys in the data set.
        """
        seen_keys = set()
        for record in self:
            if isinstance(record, dict):
                for key in record.keys():
                    if key not in seen_keys:
                        seen_keys.add(key)
                        yield key

    @requires_flatten
    def add_keys(self, keys: List[str] or str, default_value: Any = None, clobber: bool = False) -> 'DataSet':
        """
        Adds keys to the data set.

        Arguments
        keys (List[str] or str): The keys to add.
        default_value (Any, optional): The default value to assign to the keys. Defaults to None.
        clobber (bool, optional): When true, existing keys will be overwritten. Defaults to False.
        """

        if isinstance(keys, str):
            keys = [keys]

        [
            record.assign(key, default_value)
            for record in self
            for key in keys
            if clobber or record.walk(key) is None
        ]

        return self

    def add_records(self, records: dict or List[dict]) -> 'DataSet':
        """
        Adds records to the data set. Accepts either a record or a list of records.

        Arguments
        records (dict or List[dict]): The record(s) to add.
        """

        if isinstance(records, WalkableDict):
            self.append(records)

        elif isinstance(records, dict):
            self.append(WalkableDict(records))

        elif isinstance(records, (list, tuple)):
            [
                self.add_records(record)
                for record in records
            ]

        return self

    def append_record_maths_results(self, record_identifier_key: str = '_id', record_identifier_value: str = 'Totals'):
        """
        Appends the maths results to the data set.
        """

        if self.maths_results:
            self.maths_results[record_identifier_key] = record_identifier_value

            self.append(self.maths_results)

        return self

    def cast_key(self, source_key: str, target_type: CAST_TYPES or str, target_key: str = None) -> 'DataSet':
        """
        Converts a key's value to a target type. If the value cannot be converted, None is returned which is consistent
        with the `data_model.functions.cast()` method.

        Arguments
        source_key (str): The key to convert.
        target_type (str): The target type to convert the value to.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        """

        target_key = target_key or source_key

        from functions import cast

        [
            record.assign(target_key, cast(record.walk(source_key), target_type))
            for record in self
        ]

        return self

    @requires_flatten
    def clean_keys(self,
                   base_keys: list = None,
                   add_keys: list = None,
                   exclude_keys: list = None) -> 'DataSet':
        """
        Clean the keys of the record set by consolidating keys from multiple sources and excluding keys. In
        this method, 'header' is synonymous with 'key'.

        Arguments
        base_keys (list, optional): A list of keys to use as the base keys. Defaults to a consolidated list of all keys in the HarvestRecordSet.
        add_keys (list, optional): A list of keys to add to the base keys. Defaults to None.
        exclude_keys (list, optional): A list of keys to exclude from the record set. Defaults to None.
        """

        # Combine the base keys with any additional keys
        keys = (base_keys or list(self.keys)) + (add_keys or [])

        # Remove any keys that are in the exclude list
        for header in exclude_keys or []:
            if header in keys:
                keys.remove(header)

        # Generates a list of keys where subsequent instances of the header name are removed
        consolidated_keys = []

        # Adds all keys to the consolidated list
        for header in keys:
            if header not in consolidated_keys:
                consolidated_keys.append(header)

        for record in self:
            # Removes all keys that are not in the consolidated list
            for key in list(record.keys()):
                if key not in consolidated_keys:
                    record.drop(key)

            # Adds all keys that are in the consolidated list but not in the record
            for key in consolidated_keys:
                if key not in record:
                    record[key] = None

        return self

    def convert_list_of_dict_to_dict(self, source_key: str, target_key: str = None, name_key: str = 'Name', value_key: str = 'Value') -> 'DataSet':
        """
        Converts a list of dictionaries to a dictionary of key value pairs. A common example of this usage is converting
        AWS Tags lists of [{Key: 'Name', Value: 'MyName'}] to {'Name': 'MyName'}.

        Arguments
        source_key (str): The key to convert.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        name_key (str, optional): The key to use for the dictionary key. Defaults to 'Key'.
        value_key (str, optional): The key to use for the dictionary value. Defaults to '
        """

        target_key = target_key or source_key

        from functions import key_value_list_to_dict

        for record in self:
            # Gets the list of dictionaries from the source key
            list_of_dicts = record.walk(source_key, [])

            # Assigns the dictionary to the target key
            record.assign(target_key, key_value_list_to_dict(list_of_dicts, name_key, value_key))

        return self

    def convert_list_to_string(self, source_key: str, target_key: str = None, separator: str = '\n') -> 'DataSet':
        """
        Converts a list to a string. If the key is not a list or tuple, it is left as is.

        Arguments
        source_key (str): The key to convert.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        separator (str, optional): The separator to use when converting the list to a string. Defaults to '\n'.
        """

        target_key = target_key or source_key

        for record in self:
            source_value = record.walk(source_key, [])

            if isinstance(source_value, (list, tuple)):
                source_value = separator.join(source_value)

            record.assign(target_key, source_value)

        return self

    def convert_string_to_list(self, source_key: str, target_key: str = None, separator: str = ',') -> 'DataSet':
        """
        Converts a string to a list. If the key is not a string, it is left as is.

        Arguments
        source_key (str): The key to convert.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        separator (str, optional): The separator to use when converting the string to a list. Defaults to ','.
        """
        target_key = target_key or source_key

        for record in self:
            # Get the value from the source key
            if isinstance(record.get(source_key), str):
                result = record.walk(source_key, '').split(separator)

            else:
                result = record.walk(source_key, [])

            # Assign the result to the target key
            record.assign(target_key, result)

        return self

    def copy_key(self, source_key: str, target_key: str) -> 'DataSet':
        """
        Copies a key from one record to another.

        Arguments
        source_key (str): The key to copy.
        target_key (str): The key to copy to.
        """

        [
            record.assign(target_key, record.walk(source_key))
            for record in self
        ]

        return self

    def copy_record(self, source_index: int, target_index: int = None) -> 'DataSet':
        """
        Copies a record from one index to another. If the target index is not provided, the record is appended to the
        data set.

        Arguments
        source_index (int): The index of the record to copy.
        target_index (int, optional): The index to copy the record to.
        """

        from copy import copy

        if source_index < len(self):
            if target_index < len(self):
                self.insert(target_index, copy(self[source_index]))

            else:
                self.append(copy(self[source_index]))

        return self

    def create_key_from_keys(self, source_keys: List[str], target_key: str, separator: str = '-') -> 'DataSet':
        """
        Creates a key from multiple keys.

        Arguments
        source_keys (List[str]): The keys to use to create the new key.
        target_key (str): The key to create.
        separator (str, optional): The separator to use when creating the new key. Defaults to '-'.
        """

        for record in self:
            new_key_parts = []

            for key in source_keys:
                key_value = record.walk(key)

                if key_value:
                    new_key_parts.append(key_value)

            # Assign the new key to the record
            record.assign(target_key, separator.join(new_key_parts))

        return self

    def deserialize_key(self, source_key: str, target_key: str = None) -> 'DataSet':
        """
        Deserializes a key from a record. If the key is not a valid JSON string, it is left as is.

        Arguments
        source_key (str): The key to deserialize.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        """

        target_key = target_key or source_key

        from json import loads, JSONDecodeError

        for record in self:
            # Get the value from the source key
            source_value = record.walk(source_key)

            try:
                source_value = loads(source_value)

            except JSONDecodeError:
                # Don't take any action if the value is not a valid JSON string
                pass

            finally:
                record.assign(target_key, source_value)

        return self

    @requires_flatten
    def drop_keys(self, keys: List[str] or str) -> 'DataSet':
        """
        Drops keys from the data set.

        Arguments
        keys (List[str]): The keys to drop.
        """

        if isinstance(keys, str):
            keys = [keys]

        [
            record.drop(key)
            for record in self
            for key in keys
        ]

        return self

    def flatten(self, preserve_lists: bool = False, separator: str = '.') -> 'DataSet':
        """
        Flattens all records in the data set.

        Arguments
        preserve_lists (bool, optional): When true, lists will not be flattened. Defaults to False.
        separator (str, optional): The separator to use when flattening the records.
        """

        from flatten_json import flatten, flatten_preserve_lists

        # Flatten each record in the data set
        flat_data = [
            WalkableDict(flatten_preserve_lists(nested_dict=record, separator=separator)) if preserve_lists else WalkableDict(flatten(nested_dict=record, separator=separator))
            for record in self
        ]

        # Clear the data set and add the flattened data
        self.clear()
        self.add_records(flat_data)

        return self

    def limit(self, limit: int) -> 'DataSet':
        """
        Limits the number of records in the data set.

        Arguments
        limit (int): The maximum number of records to keep.
        """

        self[:] = self[:limit]

        return self

    def match_and_remove(self, matching_expressions: MatchSetGroup or List[List[str]], invert_results: bool = False) -> 'DataSet':
        """
        Evaluates expressions against the data set and removes records that do not match the expressions.

        Arguments
        matching_expressions (list or str or HarvestMatchSet): The expressions to match the records by.
        invert_results (bool, optional): When true, the results are inverted. Defaults to False.
        """
        from filters import match_records

        self[:] = list(match_records(dataset=self, match_set_group=matching_expressions, invert_results=invert_results))

        return self

    def maths_keys(self,
                   source_keys: List[str] or str,
                   target_key: str,
                   operation: VALID_MATHS_OPERATIONS,
                   missing_value: Any,
                   default_value: Any = None) -> 'DataSet':
        """
        Performs a mathematical operation on multiple keys in a record and assigns the result to a new key.

        Arguments
        source_keys (List[str] or str): The keys to perform the operation on. This is also the order of the values in the operation.
        target_key (str): The key to assign the result to.
        operation (VALID_MATHS_OPERATIONS): The operation to perform.
        missing_value (Any): The value to use when a key is missing.
        default_value (Any, optional): The default value to assign to the target key. Defaults to None.
        """

        for record in self:
            values = [
                record.walk(key, missing_value)
                for key in source_keys
            ]

            record.assign(target_key, perform_maths_operation(operation=operation, values=values) or default_value)

        return self

    def maths_records(self,
                   source_key: str,
                   target_key: str,
                   operation: VALID_MATHS_OPERATIONS,
                   missing_value: Any,
                   default_value: Any = None) -> 'DataSet':

        """
        Performs a mathematical operation on all values in a key in the data set. Results are stored in the maths_results
        dictionary and made available by accessing that attribute.

        Arguments
        source_key (str): The key to perform the operation on.
        target_key (str): The key to assign the result to in the maths_results attribute.
        operation (VALID_MATHS_OPERATIONS): The operation to perform.
        missing_value (Any): The value to use when a key is missing.
        default_value (Any, optional): The default value to assign to the target key. Defaults to None.
        """

        values = [
            record.walk(source_key, missing_value)
            for record in self
        ]

        result = perform_maths_operation(operation=operation, values=values) or default_value

        self.maths_results[target_key] = result

        return self

    def maths_reset(self):
        """
        Resets the maths_results dictionary.
        """

        self.maths_results = {}

        return self

    def nest_records(self,
                     target_key: str,
                     key_pattern: str = None,
                     nest_type: Literal['dict', 'list'] = 'dict') -> 'DataSet':
        """
        Nest keys in the record set under a new key.

        :param target_key: the key to nest the keys under
        :param key_pattern: the pattern to match keys to nest, defaults to None
        :param nest_type: the type of nesting to use, either 'dict' or 'list', defaults to 'dict'
        """

        for record in self:
            nested_result = {} if nest_type == 'dict' else []

            for key in list(record.keys()):
                if key_pattern:
                    from re import match
                    if match(key_pattern, key):
                        if isinstance(nested_result, dict):
                            nested_result[key] = record.pop(key, None)

                        else:
                            nested_result.append({key: record.pop(key, None)})

            record[target_key] = nested_result

        return self

    def remove_duplicate_records(self) -> 'DataSet':
        """
        Removes duplicate records from the data set.
        """

        from json import dumps, loads

        records_as_str = [
            dumps(record, default=str)
            for record in self
        ]

        unique_records = list(set(records_as_str))

        records = [
            loads(record)
            for record in unique_records
        ]

        self.clear()
        self.add_records(records)

        return self

    def remove_duplicates_from_list(self, source_key: str, target_key: str = None):
        """
        Removes duplicate values from a list in a record.

        Arguments
        source_key (str): The key to remove duplicates from.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        """

        target_key = target_key or source_key

        for record in self:
            source_data = record.walk(source_key)

            if isinstance(source_data, (list, tuple)):
                source_data = list(set(record.get(source_key)))

            record.assign(target_key, source_data)

        return self

    def remove_record(self, index: int = None) -> 'DataSet':
        """
        Removes a record from the data set based on the record's index.

        Arguments
        index (int, optional): The index of the record to remove
        """

        if index < len(self):
            self.pop(index)

        return self

    def rename_keys(self, mapping: dict) -> 'DataSet':
        """
        Renames keys in the data set where the mapping keys are the old key names and the key values are the new key names.

        Example
        ```python
        rename_keys({
            'old_key': 'new_key',
            'another_old_key': 'another_new_key'
        })
        ```

        Arguments
        mapping (dict): A dictionary of old key names and new key names.
        """

        for record in self:
            for old, new in mapping.items():
                record.assign(new, record.drop(old))

        return self

    def serialize_key(self, source_key: str, target_key: str = None) -> 'DataSet':
        """
        Serializes a key's value in a record to a JSON string. If the value is not a valid JSON string, it is left as is.

        Arguments
        source_key (str): The key to serialize.
        target_key (str, optional): The key to copy to. If not provided, the source key is used.
        """

        target_key = target_key or source_key

        from json import dumps, JSONEncoder

        for record in self:
            source_data = record.walk(source_key)

            try:
                source_data = dumps(source_data, default=str)

            except JSONEncoder:
                # Do nothing if the value is not a valid JSON string
                pass

            finally:
                record.assign(target_key, source_data)

        return self

    def sort_records(self, keys: List[str]) -> 'DataSet':
        """
        Sort the records in the record set by one or more keys.

        Arguments
        keys (List[str]): A list of keys to sort by.
        """

        sorted_keys = {}
        for s in keys:
            if ':' in s:
                key, value = s.split(':')
                key = key.strip()

                if value.lower() in ('dsc', 'desc'):
                    order = -1
                else:
                    order = 1
            else:
                key = s
                order = 1

            sorted_keys[key] = order

        from natsort import natsorted, ns
        self[:] = natsorted(
            self,
            key=lambda record: [
                record.walk(k, '') if direction == 1 else record.walk(k, '')[::-1]
                for k, direction in sorted_keys.items()
            ]
        )

        return self

    def split_key(self, source_key: str, target_key: str = None, separator: str = '.') -> 'DataSet':
        """
        Splits a key's value in a record by a separator.

        Arguments
        source_key (str): The key to split.
        target_key (str, optional): The key to assign the split value to.
        separator (str, optional): The separator to split the value by. Defaults to '.'.
        """

        target_key = target_key or source_key

        for record in self:
            source_data = record.walk(source_key)

            if isinstance(source_data, str):
                source_data = source_data.split(separator)

            record.assign(target_key, source_data)

        return self

    def splice_key(self, source_key: str, target_key: str = None, start: int = None, end: int = None, step: int = None) -> 'DataSet':
        """
        Splices a key's value in a record and assigns the result to a new key.

        Arguments
        source_key (str): The key to substring.
        target_key (str, optional): The key to assign the substring value to. If not provided, the source key is used.
        start (int, optional): The starting index of the substring.
        end (int, optional): The ending index of the substring.
        step (int, optional): The step of the substring.
        """
        target_key = target_key or source_key

        for record in self:
            source_data = record.walk(source_key)

            if hasattr(source_data, '__getitem__'):
                source_data = source_data[start:end:step]

            record.assign(target_key, source_data)

        return self

    @requires_flatten
    def title_keys(self, remove_characters: List[str] = None, replacement_character: str = ''):
        """
        Titles all keys in the data set, removing unwanted characters and replacing them with a provided character.

        Arguments
        remove_characters (List[str], optional): A list of characters to remove from the keys. Defaults to None.
        replacement_character (str, optional): The character to replace the removed characters with. Defaults to ''.
        """

        for record in self:
            for key in list(record.keys()):
                new_key = key.title()

                if remove_characters:
                    for character in remove_characters:
                        new_key = new_key.replace(character, replacement_character)

                record[new_key] = record.pop(key)

        return self

    def to_redis(self, name_key: str) -> 'DataSet':
        """
        Convert the data set to a Redis-compatible format. This permanently modifies the record set.

        :param name_key: The key to store the record set under
        """
        from json import dumps

        result = [
            WalkableDict({
                record[name_key]: {
                    key: dumps(value, default=str)
                    for key, value in record.items()
                }
                for record in self
            })

        ]

        # Remove the original records
        self.clear()

        # Add the new records
        self.add_records(result)

        return self

    def unflatten(self, separator: str = '.') -> 'DataSet':
        """
        Unflattens all records in the data set.

        Arguments
        separator (str, optional): The separator to use when unflattening the records.
        """

        from flatten_json import unflatten_list

        # Unflatten each record in the data set
        unflat_data = [
            WalkableDict(unflatten_list(record, separator=separator))
            for record in self
        ]

        # Clear the data set and add the unflattened data
        self.clear()
        self.add_records(unflat_data)

        return self

    def unnest_records(self, key_pattern: str = None, nest_level: int = 0, delimiter: str = '.') -> 'DataSet':
        """
        Removes the indicated layers of nesting from records.

        Arguments:
        key_pattern (str): The pattern to match keys to denormalize. When not provided, all keys are affected.
        nest_level (int): The number of layers to remove from the key. Defaults to 0.
        delimiter (str): The delimiter used to separate keys. Defaults to '.'.
        """
        from flatten_json import flatten, unflatten_list

        # Flatten the records
        flat_records = [
            flatten(nested_dict=record, separator=delimiter)
            for record in self
        ]

        # Denormalize the keys
        for record in flat_records:
            for key in list(record.keys()):
                if key_pattern:
                    from re import fullmatch
                    if fullmatch(key_pattern, key):
                        new_key = delimiter.join(key.split(delimiter)[-nest_level:])
                        record[new_key] = record.pop(key, None)

        # Unflatten the records
        result = [
            unflatten_list(record, separator=delimiter)
            for record in flat_records
        ]

        self.clear()
        self.add_records(result)

        return self

    def unwind(self, source_key: str, preserve_null_and_empty_keys: bool = False) -> 'DataSet':
        """
        Unwind a list of records in the data set into separate records.

        Arguments
        source_key (str): The key to unwind.
        preserve_null_and_empty_keys (bool, optional): When true, null and empty keys will be preserved. Defaults to False.
        """

        new_records = []
        for record in self:
            if source_key not in record.keys() and preserve_null_and_empty_keys is False:
                continue

            elif isinstance(record.get(source_key), (list or tuple)):
                for item in record[source_key]:
                    new_record = record.copy()
                    new_record[source_key] = item
                    new_records.append(new_record)

            else:
                new_records.append(record)

        self.clear()
        self.add_records(new_records)

        return self

    def wind(self, source_key: str, preserve_null_and_empty_values: bool = False) -> 'DataSet':
        """
        Wind a list of records in the data set into a single record.

        Arguments
        source_key (str): The key to wind.
        preserve_null_and_empty_keys (bool, optional): When true, null and empty keys will be preserved. Defaults to False.
        """

        # First we must generate a dictionary of all the records excluding the source key where the record itself is
        # the key and the value is a list of the source key values. This will allow us to group the records by the
        # source key value while retaining the other key values.

        record_index = {}

        from json import dumps, loads

        for record in self:
            # Get the value of this record's source key
            source_key_value = record.pop(source_key, None)

            # Generate a dictionary key from the record
            record_index_key = dumps(record, default=str)

            # Make sure the record index value is a list
            if record_index_key not in record_index.keys():
                record_index[record_index_key] = []

            # Skip the record if the source key value is None and preserve_null_and_empty_values is False
            if source_key_value is None and preserve_null_and_empty_values is False:
                continue

            # Add the source key value to the record index
            else:
                record_index[record_index_key].append(source_key_value)

        # Now we can generate the new records
        new_records = []

        for jsonified_record, source_key_values in record_index.items():
            # Convert the record back to a dictionary
            record = loads(jsonified_record)

            # Add the source key values to the record
            record[source_key] = source_key_values

            # Add the record to the new records
            new_records.append(record)

        # Purge the existing records from the data set and add the new records
        self.clear()
        self.add_records(new_records)

        return self


def perform_maths_operation(operation: VALID_MATHS_OPERATIONS, values: List[Any]) -> int or float or None:
    """
    Perform the mathematical operation on the values.

    Arguments
    operation (VALID_MATHS_OPERATIONS): The operation to perform.
    values (List[Any]): The values to perform the operation on.
    """

    total = None

    try:
        if operation == 'add':
            total = sum(values)

        elif operation == 'subtract':
            total = 0
            for value in values:
                total -= value

        elif operation == 'multiply':
            total = 1
            for value in values:
                total *= value

        elif operation == 'divide':
            total = values[0]
            for value in values[1:]:
                total /= value

        elif operation == 'average':
            total = sum(values) / len(values)

        elif operation == 'minimum':
            total = min(values)

        elif operation == 'maximum':
            total = max(values)

    finally:
        return total
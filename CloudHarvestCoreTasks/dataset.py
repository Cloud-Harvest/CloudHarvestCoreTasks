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

def rebuild_indexes(method):
    """
    Decorator to rebuild the indexes after a method is called.

    Arguments
    method (function): The method to decorate.
    """

    def wrapper(self, *args, **kwargs):
        # Call the method
        result = method(self, *args, **kwargs)

        # Rebuild the indexes
        [self.refresh_index(index_name) for index_name in self.indexes.keys()]

        return result

    return wrapper

# CLASSES---------------------------------------------------------------------------------------------------------------
class WalkableDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def assign(self, key: str, value: Any, separator: str = '.') -> dict:
        """
        Assigns a value to a key in a self.

        Arguments:
        key (str): The key to assign the value to.
        value (Any): The value to assign.
        separator (str, optional): The separator to use when splitting the key. Defaults to '.'.
        """
        def recursive_assign(target, path):
            part = path[0]
            # If this is the last part of the path, assign the value
            if len(path) == 1:
                if isinstance(target, dict):
                    target[part] = value
                elif isinstance(target, list) and part.isdigit():
                    index = int(part)
                    if 0 <= index < len(target):
                        target[index] = value
                return

            # If the target is a dictionary, ensure the next part exists
            if isinstance(target, dict):
                if part not in target:
                    target[part] = {} if not path[1].isdigit() else []
                recursive_assign(target[part], path[1:])

            # If the target is a list, ensure the next part exists
            elif isinstance(target, list) and part.isdigit():
                index = int(part)
                if 0 <= index < len(target):
                    if not isinstance(target[index], (dict, list)):
                        target[index] = {} if not path[1].isdigit() else []
                    recursive_assign(target[index], path[1:])

        # If the key does not contain the separator, assign the value directly
        if separator not in key:
            self[key] = value
            return self

        # Split the key into parts and recursively assign the value
        p = key.split(separator)
        recursive_assign(self, p)
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

    def map(self, item: Any = None, nested: bool = False) -> Any:
        """
        Recursive method which creates a map of the self, replacing values with data types.
        """

        if item is None and not nested:
            item = self

        if isinstance(item, dict):
            return {
                key: self.map(value, nested=True)
                for key, value in item.items()
            }

        elif isinstance(item, list):
            list_result = []
            for value in item:
                list_result.append(self.map(value, nested=True))

            return list(set(list_result))

        else:
            return type(item).__name__

    def replace(self, variables: dict, die_on_unassigned: bool = False) -> 'WalkableDict':
        """
        Replaces string representations with variables in the self.

        Arguments
        variables (dict): The variable mapping used to replace string expressions in self. Each key in the dictionary
        serves as a prefix for the variable name, and the corresponding value is the variable's value.
        die_on_unassigned (bool, optional): When true, an exception is raised if a variable is not found. Defaults to False.
        ```
        {
            "env": { }
            "var": {
                "my_var_1": "something",
                "my_var_2": "something else"
            }
            "item": { }
        }
        ```
        """
        from flatten_json import flatten
        from re import findall, sub

        # If there are no keys in either dictionary, nothing can be replaced
        if not self.keys() or not variables.keys():
            return self

        # Convert the variables to a WalkableDict to allow for nested key access
        variables = WalkableDict(variables)

        # Create a regex expression to match the variable prefixes
        regex_expression = r'\b(?:' + '|'.join(variables.keys()) + r')\.[^\s]*'

        # Flatten the self which will give us the key paths and values. We do not iterate over the values or unflatten
        # this list because unflatten will fail if a replacement variable contains a list or dictionary structure.
        flat_self = flatten(self, separator='.')

        # Iterate through the flattened keys (effectively a list of self's paths) and check if the value is a string
        for key in flat_self.keys():
            # Starting value. We always walk to self's value because this is also the value we assign to the self key.
            # Using the flat_self value would not work because we never change this value.
            value = self.walk(key)

            if isinstance(value, str):
                # Iterate over matches in the regex expression
                for match in findall(regex_expression, value) or []:
                    value = self.walk(key)
                    replacement_value = variables.walk(match)

                    # If the replacement value is None, continue or die as specified
                    if replacement_value is None:
                        if die_on_unassigned:
                            # When die_on_unassigned is true, raise an exception if the variable is not found
                            raise ValueError(f'Variable `{match}` not found when replacing in `{key}`')

                        else:
                            # If the replacement value is None, skip to the next match. This allows us to support scenarios
                            # where variables yet to be assigned can be used in the string in subsequent iterations
                            continue

                    # If the match and value are the same, assign the value to the key
                    if match == value:
                        self.assign(key, replacement_value)

                    # Otherwise the variable reference is inside an existing string. Therefore, we replace the matching
                    # variable name with a string representation of the variable value
                    else:
                        # Sub is used to replace whole words only
                        self.assign(key, sub(pattern=r'\b' + match + r'\b', repl=str(replacement_value), string=str(value)))

        return self

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
            v = self.get(key)

            if v is None:
                return default

            else:
                return v

        # Split the key into individual path using the separator
        path = key.split(separator)
        result = default

        # Start with the base self object as we walk the path to the target value
        target = self

        for part in path:
            try:
                # If the target is a dictionary, get the indicated key (part).
                if isinstance(target, dict):
                    # Perform a string comparison of the part and the keys in the target dictionary. This replaces
                    # the 'part in target.keys()' check in the original code which was insufficient for comparisons when
                    # the part or key was an integer
                    found_part = [
                        key for key in target.keys()
                        if str(key) == str(part)
                    ]

                    if found_part:
                        if len(found_part) == 1:
                            part = found_part[0]

                        else:
                            # This can only happen if 'part' is represented as an integer and a string representation
                            # within the dictionary keys. When this happens, we will continue to use the original part
                            # as defiled in the path.
                            pass

                        # If the part is a key, get the value
                        target = target[part]

                    elif hasattr(str(target), part):
                        # If the part is an attribute, get the attribute value
                        target = getattr(str(target), part)

                    else:
                        # If the part is not found, return the default value
                        return default

                # If the target is a list and the part is an integer, treat it as an index
                elif isinstance(target, (list, tuple, str)):
                    if part.isdigit():
                        # If the part is an integer, treat it as an index
                        index = int(part)

                        if 0 <= index < len(target):
                            target = target[index]

                        else:
                            # If the index is out of bounds, return the default value
                            return default

                    elif hasattr(str(target), part):
                        # If the part is a string, treat it as an attribute
                        target = getattr(str(target), part)

                    else:
                        # If the part is not found, return the default value
                        return default

                else:
                    # Check we're at the end of the path
                    if path.index(part) == len(path) - 1:
                        # If we've reached the end of the path, return the target value
                        pass

                    elif hasattr(str(target), part):
                        # If the part is a string, treat it as an attribute
                        target = getattr(str(target), part)

                    else:
                        # If the part is not found, return the default value
                        return default

            except BaseException as ex:
                # If an exception occurs, return the default value
                return default

            else:
                result = target

        return result


class DataSet(List[WalkableDict]):
    from CloudHarvestCoreTasks.functions import CAST_TYPES

    def __init__(self, *args):
        super().__init__()

        if args:
            self.add_records(args)

        self.indexes = {}
        self.maths_results = WalkableDict()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()
        return None

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

        from CloudHarvestCoreTasks.functions import cast

        [
            record.assign(target_key, cast(record.walk(source_key), target_type))
            for record in self
        ]

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

        from CloudHarvestCoreTasks.functions import key_value_list_to_dict

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
            if target_index and target_index < len(self):
                self.insert(target_index, copy(self[source_index]))

            else:
                self.append(copy(self[source_index]))

        return self

    def count_elements(self, source_key: str, target_key: str = None) -> 'DataSet':
        """
        Counts the number of elements in an object and assigns the result to a new key.

        Arguments
        source_key (str): The key to count.
        target_key (str, optional): The key to record the count to. If not provided, the source key is used.
        """

        target_key = target_key or source_key

        for record in self:
            source_value = record.walk(source_key, [])

            result = None

            if hasattr(source_value, '__len__'):
                result = len(source_value)

            record.assign(target_key, result)

        return self

    def create_index(self, name: str, keys: List[str]) -> 'DataSet':
        """
        Creates an index on the data set. Indexes are string representations of the keys in the data set. The index is
        created by concatenating the keys with a separator. Indexes are not automatically updated when the data set is
        modified.

        Arguments
        name (str): The name of the index.
        keys (List[str]): The keys to use to create the index.
        """

        index_name = name
        result = {
            'keys': [keys] if isinstance(keys, str) else keys,
            'values': {}
        }

        for record in self:
            # Identify the index value for the record
            record_index_value = '-'.join([str(record.walk(key, default='None')) for key in keys])

            # Creates the index value if it does not exist
            if record_index_value not in result['values'].keys():
                result['values'][record_index_value] = []

            # Append the record to the index. Python will use pointers here so the record is not copied.
            result['values'][record_index_value].append(record)

        # Store the index in the indexes attribute
        self.indexes[index_name] = result

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

        from json import loads

        for record in self:
            # Get the value from the source key
            source_value = record.walk(source_key)

            try:
                source_value = loads(source_value)

            except Exception:
                # Don't take any action if the value is not a valid JSON string
                pass

            finally:
                record.assign(target_key, source_value)

        return self

    def drop_index(self, name) -> 'DataSet':
        """
        Drops an index from the data set.

        Arguments
        name (str): The name of the index to drop.
        """

        if name in self.indexes.keys():
            del self.indexes[name]

        return self

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

    def find_index(self, keys: List[str], create: bool = False) -> str or None:
        """
        Finds an index in the data set based on the keys used.

        Arguments
        keys (List[str]): The keys to use to find the index.
        create (bool, optional): When true, a new index is created if it does not exist. Defaults to False.
        """

        keys = [keys] if isinstance(keys, str) else keys

        for index_name, index in self.indexes.items():
            if index['keys'] == keys:
                return index_name

        if create:
            name = '_'.join(keys) + '_index'
            self.create_index(name=name, keys=keys)
            return name

        return None

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

    def join(self, data: 'DataSet', left_keys: List[str], right_keys: List[str], inner: bool = False, target_key: str = None) -> 'DataSet':
        """
        Merges two DataSets based on the specified keys. The left DataSet is the one that calls this method while the
        right DataSet is passed as an argument. The join is performed by matching the values of the specified keys in
        both DataSets. The resulting DataSet contains all records from the left DataSet and the matching records from
        the right DataSet. When keys exist in both the left- and right-handed DataSets, the right-handed DataSet's
        values are used.

        When the `inner` argument is True, only records that exist in both DataSets are included in the result. This is
        also known as an inner join. When `inner` is False, all records from the left DataSet are included in the result,

        Arguments
        data (DataSet): The DataSet to join into this one.
        left_keys (str): The keys used on the left side of the join
        right_keys (str): The keys used on the right side of the join
        inner (bool): When true, an inner join is performed. Defaults to False.
        target_key (str): The key to assign the matching right-hand records to. If not provided, the right-hand records are merged into the original record.
        """

        data = DataSet(data) if not isinstance(data, DataSet) else data

        # Find the index names for the left and right DataSets
        left_index_name = self.find_index(left_keys, create=True)
        right_index_name = data.find_index(right_keys, create=True)

        # Create a new DataSet to hold the joined records
        joined_data = DataSet()

        # Iterate through the left DataSet.indexes[left_index_name]['values']
        for left_index_value, left_records in self.indexes[left_index_name]['values'].items():
            # Get the corresponding records from the right DataSet using the index name
            right_records = data.indexes[right_index_name]['values'].get(left_index_value, [])

            # Iterate through each record in the left DataSet
            for left_record in left_records:
                from copy import deepcopy

                if right_records:
                    # Create a new record for each matching right-hand record
                    for right_record in right_records:
                        # Create a new record by merging the left and right records
                        merged_record = WalkableDict(deepcopy(left_record))

                        if target_key:
                            # If a target key is provided, assign the right record to the target key
                            merged_record.assign(target_key, deepcopy(right_record))

                        else:
                            # Otherwise, merge the right record into the left record
                            merged_record.update(right_record)

                        # Append the merged record to the joined_data DataSet
                        joined_data.append(merged_record)

                else:
                    # If there are no matching records in the right DataSet and an inner join is specified, skip the left record
                    # This is what makes a join an "inner" join: records must exist in both DataSets
                    if inner:
                        continue

                    # If there are no matching records in the right DataSet, append the left record to the joined_data DataSet
                    joined_data.append(deepcopy(left_record))

        # Update the DataSet with the joined data
        [self.refresh_index(index_name) for index_name in self.indexes.keys()]
        self.maths_results.clear()
        self.clear()
        self.add_records(joined_data)

        return self

    def limit(self, limit: int) -> 'DataSet':
        """
        Limits the number of records in the data set.

        Arguments
        limit (int): The maximum number of records to keep.
        """

        self[:] = self[:limit]

        return self

    def map(self, limit: int = 100) -> WalkableDict:
        """
        Creates a map of the dataset's structure, replacing values with data types and merging those data types into a
        single map.
        """

        def walk_and_merge(value, merged):
            if isinstance(value, dict):
                for k, v in value.items():
                    if k not in merged:
                        merged[k] = v
                    else:
                        merged[k] = walk_and_merge(v, merged[k])
            elif isinstance(value, list):
                if not isinstance(merged, list):
                    merged = []
                for item in value:
                    if item not in merged:
                        merged.append(item)
            else:
                merged = value
            return merged

        combined_result = WalkableDict()
        for record in self[:limit]:
            mapped = record.map()
            walk_and_merge(mapped, combined_result)

        return combined_result

    def match_and_remove(self, matching_expressions: List[List[str]], filterable_fields: List[str] = None, invert_results: bool = False) -> 'DataSet':
        """
        Evaluates expressions against the data set and removes records that do not match the expressions.

        Arguments
        filterable_fields (List[str], optional): The fields to filter by. If not provided, all fields can be filtered.
        matching_expressions (list or str or MatchSetGroup): The expressions to match the records by.
        invert_results (bool, optional): When true, the results are inverted. Defaults to False.
        """
        if not isinstance(matching_expressions, MatchSetGroup):
            matching_expressions = MatchSetGroup(matching_expressions, filterable_fields=filterable_fields)

        self[:] = [
            record
            for record in self
            if matching_expressions.evaluate(record) ^ invert_results
        ]

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

    def merge_keys(self, source_keys: List[str] or str, target_key: str = None, preserve_original_keys: bool = False) -> 'DataSet':
        """
        Merges source keys into a target key. If the target_key is not provided, the source keys are merged into the
        top of the record, effectively unnesting those keys.

        :param source_keys: the keys to merge
        :param target_key: the key to merge the source keys into. When not provided, the source keys are placed at the top level of the record
        :param preserve_original_keys: when true, the original keys are preserved in the record
        """

        if isinstance(source_keys, str):
            source_keys = [source_keys]

        for record in self:
            merged_result = {}
            for key in source_keys:
                value = record.walk(key)

                if isinstance(value, dict):
                    merged_result.update(value)

            if target_key:
                record.assign(target_key, merged_result)

            else:
                record.update(merged_result)

            if not preserve_original_keys:
                for key in source_keys:
                    record.drop(key)

        return self

    def nest_keys(self, source_keys: List[str], target_key: str = None, preserve_original_keys: bool = False) -> 'DataSet':
        """
        Nest keys in the record set under a new key.

        :param source_keys: the keys to nest under the target_key
        :param target_key: the key to nest the keys under. When not provided, the source keys are placed at the top level of the record
        :param preserve_original_keys: when true, the original keys are preserved in the record
        """

        if isinstance(source_keys, str):
            source_keys = [source_keys]

        for record in self:
            nested_result = {
                key: record.walk(key) if preserve_original_keys else record.drop(key, None)
                for key in source_keys
            }

            if target_key:
                record.assign(target_key, nested_result)

            else:
                record.update(nested_result)

        return self

    def refresh_index(self, name: str) -> 'DataSet':
        """
        Refreshes the index in the data set.

        Arguments
        name (str): The name of the index to refresh.
        """

        if name in self.indexes.keys():
            self.create_index(name, self.indexes[name]['keys'])

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

    def set_keys(self, keys: List[str]) -> 'DataSet':
        """
        Rebuilds the data set with only the provided keys. Note that the keys will not be ordered as provided. This would
        require an OrderedDict; however, WalkableDict is not an OrderedDict. Further, since we intend to transit the
        data set through Agent or API channels, we can expect the keys to be reordered during the JSON encode/decode
        process. Therefore, it is incumbent upon the consumer to reorder the keys, if applicable.

        Arguments
        keys (List[str]): The keys to keep.
        """

        # If no keys are provided, return the data set as is
        if not keys:
            return self

        results = []
        for record in self:
            new_record = WalkableDict()
            for key in keys:
                new_record.assign(key, record.walk(key))

            results.append(new_record)

        self.clear()
        self.add_records(results)

        return self

    def slice_records(self, start_index: int = None, end_index: int = None) -> 'DataSet':
        """
        Only keeps the records between the start and end index.

        Arguments
        start_index (int, optional): The starting index. Defaults to None.
        end_index (int, optional): The ending index. Defaults to None.
        """

        self[:] = self[start_index:end_index]

        return self

    def split_key_to_keys(self, source_key: str, target_keys: List[str], separator: str = '.', max_split: int = None, default_value: Any = None, preserve_source_key: bool = False) -> 'DataSet':
        """
        Splits a key's value in a record into multiple keys. The split value is assigned to the target keys. If there are
        not enough values to assign to the target keys, the remaining target keys are assigned to the default value.

        Arguments
        source_key (str): The key to split.
        target_keys (List[str]): The keys to assign the split value to.
        separator (str, optional): The separator to split the value by. Defaults to '.'.
        max_split (int, optional): The maximum number of times to split the separator. Defaults to None which means no limit.
        default_value (Any, optional): The default value to assign to the target keys. Defaults to None.
        preserve_source_key (bool, optional): When true, the source key will be preserved. Defaults to False.
        """

        for record in self:
            source_data = record.walk(source_key)

            # Can only split on a string
            if isinstance(source_data, str):
                source_data = source_data.split(separator, maxsplit=max_split or -1)    # maxsplit=None raises an error

            # But we can still support list/tuple
            elif isinstance(source_data, (list, tuple)):
                source_data = list(source_data)

            # If the source data is not a string, list, or tuple, set it to an empty list. This allows us to assign the
            # default value to the target keys
            else:
                source_data = []

            # Always add the default values to the target keys, even if the source data is empty
            for i, target_key in enumerate(target_keys):
                try:
                    record.assign(target_key, source_data[i])

                except IndexError:
                    record.assign(target_key, default_value)

            # Remove the source key from the record if it is not preserved
            if not preserve_source_key:
                record.drop(source_key, None)

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

    def title_keys(self, remove_characters: List[str] = None, replacement_character: str = ''):
        """
        Titles all root keys in the data set.

        Arguments
        remove_characters (List[str], optional): A list of characters to remove from the keys. Defaults to None.
        replacement_character (str, optional): The character to replace the removed characters with. Defaults to '' for CamelCase.
        """

        for record in self:
            for key in list(record.keys()):
                # Assign the new key to a variable for further modification
                new_key = key.title()

                # Remove characters if specified
                for character in remove_characters:
                    new_key = new_key.replace(character, replacement_character)

                # Assign the new key to the record
                record[new_key] = record.pop(key)

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

    def unwind(self, source_key: str, preserve_null_and_empty_keys: bool = False) -> 'DataSet':
        """
        Unwind a list of records in the data set into separate records.

        Arguments
        source_key (str): The key to unwind.
        preserve_null_and_empty_keys (bool, optional): When true, null and empty keys will be preserved. Defaults to False.
        """

        new_records = []
        for record in self:
            if not record.walk(source_key) and preserve_null_and_empty_keys is False:
                continue

            elif isinstance(record.walk(source_key), (list or tuple)):
                for item in record[source_key]:
                    new_record = WalkableDict(record.copy())
                    new_record.assign(source_key, item)
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
            source_key_value = record.drop(source_key, None)

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
            # Convert the record back to a WalkableDict
            record = WalkableDict(loads(jsonified_record))

            # Add the source key values to the record
            record.assign(source_key, source_key_values)

            # Add the record to the new records
            new_records.append(record)

        # Purge the existing records from the data set and add the new records
        self.clear()
        self.add_records(new_records)

        return self

    def values(self, source_key: str, default: Any = None):
        """
        Consolidates the values of a key in the data set.

        Arguments
        source_key (str): The key to consolidate.
        missing_value (Any): The value to use when a key is missing.
        """

        for record in self:
            yield record.walk(source_key, default)


class Match:
    """
    A Match is the lowest-level matching class. It is used to match a key, operator, and value in a record using a
    specific operator. The `evaluate()` method is called to determine if the match is successful.
    """
    def __init__(self, syntax: str):
        """
        Initializes a new Match.

        Arguments
        syntax (str): The syntax to match.

        Examples
        >>> # Regex match of the key with the value
        >>> Match('key=value')
        >>> # Exact match of the key with the value
        >>> Match('key==value')
        >>> # Greater than or equal to match of the key with the value
        >>> Match('key>=value')
        """
        self.syntax = syntax

        # Determine the operator for the match
        self.operator, \
        self.operator_method = self.get_operator()

        # Split the syntax into key, operator, and value
        self.key, \
        self.value = self.syntax.split(self.operator)

        # Configure null values
        if self.value == 'null':
            if self.operator == '=':
                self.operator = '=='

            elif self.operator == '!=':
                self.operator = '!=='

            self.value = None

        # Configure bools
        if self.value == 'true':
            if self.operator == '=':
                self.operator = '=='

            elif self.operator == '!=':
                self.operator = '!=='

            self.value = True

        elif self.value == 'false':
            if self.operator == '=':
                self.operator = '=='

            elif self.operator == '!=':
                self.operator = '!=='

            self.value = False

    @property
    def final_operator(self):
        """
        Returns a string representation of the operator.
        """
        return ''.join([str(s) for s in [self.key, self.operator, self.value]])

    def get_operator(self) -> tuple:
        """
        Retrieves the operator key from the matching syntax.

        Returns:
            str: The operator key.

        Raises:
            ValueError: If no valid operator is found in the syntax.
        """

        import operator
        from re import findall

        # The order of match operation keys is important. The keys should be ordered from longest to shortest to ensure that
        # the longest match is attempted first. For example, '==' should be before '=' to ensure that '==' is matched
        # before '='. This allows us to perform split() operations on the syntax without accidentally splitting on a substring
        # that is part of the operator.

        from re import findall

        match_operations = {
            '!==': operator.ne,     # Checks if 'a' is not equal to 'b'
            '==': operator.eq,      # Checks if 'a' is equal to 'b'
            '>=': operator.ge,      # Checks if 'a' is greater than or equal to 'b'
            '=>': operator.ge,      # Checks if 'a' is greater than or equal to 'b'
            '<=': operator.le,      # Checks if 'a' is less than or equal to 'b'
            '=<': operator.le,      # Checks if 'a' is less than or equal to 'b'
            '!=': findall,          # Checks if 'a' does not match regex expression 'b'
            '>': operator.gt,       # Checks if 'a' is greater than 'b'
            '<': operator.lt,       # Checks if 'a' is less than 'b'
            '=': findall            # Checks if 'a' matches regex expression 'b'
        }

        for op, method in match_operations.items():
            if op in self.syntax:
                return op, method

        raise ValueError('No valid operator found in syntax. Valid operators are: ' + ', '.join(match_operations.keys()))

    def as_str(self) -> str:
        """
        Returns a string representation of the Match.
        """

        return f'{self.key}{self.operator}{self.value}'

    def evaluate(self, item: WalkableDict or dict) -> bool:
        """
        Matches the item against the Match.

        Arguments:
            item (WalkableDict): The item to be matched.

        Returns:
            bool: The result of the match.
        """
        if not isinstance(item, WalkableDict):
            item = WalkableDict(item)

        from CloudHarvestCoreTasks.functions import is_bool, is_datetime, is_null, is_number
        matching_value = self.value
        record_key_value = item.walk(self.key)

        # convert types if they do not match
        if type(matching_value) is not type(record_key_value):
            if is_bool(matching_value):
                cast_variables_as = 'bool'

            elif is_datetime(matching_value):
                cast_variables_as = 'datetime'

            elif is_null(matching_value):
                cast_variables_as = 'null'

            elif is_number(matching_value):
                cast_variables_as = 'float'

            else:
                cast_variables_as = 'str'

            from CloudHarvestCoreTasks.functions import cast
            matching_value = cast(matching_value, cast_variables_as)
            record_key_value = cast(record_key_value, cast_variables_as)

        from re import findall, IGNORECASE
        if self.operator == '=':
            result = findall(pattern=matching_value, string=record_key_value, flags=IGNORECASE)

        elif self.operator == '!=':
            result = not findall(pattern=matching_value, string=record_key_value, flags=IGNORECASE)

        else:
            result = self.operator_method(record_key_value, matching_value)

        return result


class MatchSet(List[Match]):
    """
    A MatchSet represents a list of multiple Match objects. When evaluate() is called, all Match objects are evaluated
    and the results are combined using a logical AND. If all Match objects evaluate to True, the MatchSet evaluates to True.
    Conversely, if any Match object evaluates to False, the MatchSet evaluates to False.
    """
    def __init__(self, *args):
        """
        Initializes a new MatchSet. Accepts a variable number of arguments that are converted to Match objects.
        """
        super().__init__()

        def add_match(match: Any):
            if isinstance(match, (list, MatchSet, tuple)):
                for m in match:
                    add_match(m)

            elif isinstance(match, Match):
                self.append(match)

            elif isinstance(match, str):
                self.append(Match(match))

        add_match(args)

    def as_dict(self) -> dict:
        """
        Returns a dictionary representation of the MatchSet.
        """
        return {
            "$and": [
                match.as_str()
                for match in self
            ]
        }

    def evaluate(self, item: WalkableDict or dict) -> bool:
        """
        Matches the item against the match.

        Arguments:
            item (dict): The item to be matched.

        Returns:
            bool: The result of the match.
        """

        return all(
            match.evaluate(item)
            for match in self
        )


class MatchSetGroup(List[MatchSet]):
    """
    A MatchGroup represents a list of multiple MatchSet objects. When evaluate() is called, all MatchSet objects are
    evaluated and the results are combined using a logical OR. If any MatchSet object evaluates to True, the MatchGroup
    evaluates to True. Conversely, if all MatchSet objects evaluate to False, the MatchGroup evaluates to False.
    """
    def __init__(self, *args, filterable_fields: List[str] = None):
        self.filterable_fields = filterable_fields or []
        super().__init__()

        for arg in args:
            if isinstance(arg, MatchSetGroup):
                self.extend(arg)

            elif isinstance(arg, list):
                self.append(MatchSet(*arg))

            elif isinstance(arg, str):
                self.append(MatchSet(arg))

        if self.filterable_fields:
            # When filterable_fields is provided, remove any Match objects that do not have
            # a key in the filterable fields
            for match_set in self:
                for match in list(match_set):
                    if match.key not in self.filterable_fields:
                        match_set.remove(match)

    def as_dict(self) -> dict:
        """
        Returns a dictionary representation of the MatchSetGroup.

        Returns:
            dict: A dictionary representation of the MatchSetGroup.
        """

        return {
            "$or": [
                match_set.as_dict()
                for match_set in self
            ]
        }

    def evaluate(self, item: WalkableDict or dict) -> bool:
        """
        Matches the item against the match.

        Arguments:
            item (dict): The item to be matched.

        Returns:
            bool: The result of the match.
        """

        return any(
            match_set.evaluate(item)
            for match_set in self
        )


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

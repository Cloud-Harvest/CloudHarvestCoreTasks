"""
This module defines the `HarvestRecordSet` and `HarvestRecordSets` classes for managing collections of
`HarvestRecord` objects.

Classes:
    HarvestRecordSet: A class representing a list of `HarvestRecord` objects with methods for performing operations on all records in the set.
    HarvestRecordSets: A dictionary-like class for managing multiple `HarvestRecordSet` objects.

"""

from typing import Any, Dict, List, Literal
from collections import OrderedDict


class HarvestRecord(OrderedDict):
    """
    A class representing an individual record in a HarvestRecordSet.
    """

    def __init__(self, recordset: 'HarvestRecordSet' = False, is_flat: bool = False, **kwargs):
        """
        Initialize the HarvestRecord object.
        """

        super().__init__(**kwargs)

        self.recordset = recordset
        self.is_flat = is_flat
        self.matching_expressions = []
        self.non_matching_expressions = []

    @property
    def is_matched_record(self) -> bool:
        """
        Check if the record is a match.

        :return: True if the record is a match, False otherwise
        """

        return len(self.non_matching_expressions) == 0

    def add_freshness(self, fresh_range: int = 3600, aging_range: int = 43200) -> 'HarvestRecord':
        """
        Add the freshness key to the record. Freshness is determined by the time since the record was last seen and whether the record is active.

        :param fresh_range: lower bound of the freshness range, defaults to 3600
        :param aging_range: middle and upper bound of the freshness range, defaults to 43200
        """

        from .functions import cast
        from datetime import datetime, timezone

        active = self.get('Harvest', {}).get('Dates', {}).get('Active') or self.get('Active')
        last_seen = cast(value=self.get('Harvest', {}).get('Dates', {}).get('LastSeen') or self.get('LastSeen'),
                         typeof='datetime.fromisoformat')

        result = 'I'
        if active and last_seen:
            now = datetime.now(tz=timezone.utc)
            age = (now - last_seen).total_seconds()

            # Fresh: one hour
            if age <= fresh_range:
                result = 'F'

            # Aging: twelve hours
            elif fresh_range > age > aging_range:
                result = 'A'

            # Stale: older than twelve hours
            elif age > aging_range:
                result = 'S'

            # Error: unknown state
            else:
                result = 'E'

        self['f'] = result

        return self

    def add_key_from_values(self, target_key: str, values: List[str], abort_if_null: bool = False, delimiter: str = '') -> 'HarvestRecord':
        """
        Create a new key in the record by concatenating the values of the keys provided.

        Arguments
        ---------
        target_key (str): The name of the new key.
        values (List[str]): The values to apply to the new key.
        abort_if_null (bool, optinal): If True, the method will abort if any value in the sequence is None, defaults to False
        delimiter (str, optional): The delimiter to use when concatenating the values, defaults to '' (no delimiter).

        Example
        -------
        Python
        >>> record = HarvestRecord(data={'first_name': 'John', 'last_name': 'Doe'})
        >>> record.add_key_from_values(target_key='full_name', values=['John', 'Doe'], delimiter=' ')
        >>> print(record['full_name'])
        >>> 'John Doe'

        Configuration
        -------------
        This example templates the configuration to produce the same result as the Python example above.
        ```yaml
        - add_key_from_values:
            target_key: full_name
            values:
              - {{first_name}}
              - {{last_name}}
            delimiter: ' '
        ```
        """

        result = []
        for value in values:

            # If the value is None and abort_if_null is True, the method will abort
            if abort_if_null and self.get(value) is None:
                self[target_key] = None
                return self

            # We convert the value to a string to avoid errors when joining
            result.append(str(value))

        self[target_key] = delimiter.join(result)

        return self

    def add_key_values(self, data: dict, replace_existing: bool = False) -> 'HarvestRecord':
        """
        Add multiple key-value pairs to the record.

        :param data: A dictionary containing the key-value pairs to add to the record.
        :param replace_existing: If True, existing keys in the record will be replaced by the new values. Defaults to False.
        """

        for key, value in data.items():
            if replace_existing or key not in self:
                self[key] = value

        return self

    def assign_elements_at_index_to_key(self, source_key: str, target_key: str, start: int = None, end: int = None, delimiter: str = None) -> 'HarvestRecord':
        """
        Assign elements at a specific index to a new key.

        :param source_key: the name of the source key
        :param target_key: the name of the target key
        :param start: the index start stage_position
        :param end: the index end stage_position
        :param delimiter: the delimiter to use when joining the elements, defaults to None
        """

        from collections.abc import Iterable

        result = None
        if isinstance(source_key, Iterable):
            result = self[source_key][start:end]

            if delimiter:
                result = delimiter.join(result)

        self[target_key] = result

        return self

    def assign_value_to_key(self, key: str, value: Any = None, clobber: bool = False) -> 'HarvestRecord':
        """
        Assign a value to a key in the record.

        :param key: the name of the key
        :param value: the value to assign to the key, defaults to None
        :param clobber: whether to overwrite the existing value, defaults to False
        """

        if clobber or key not in self:
            self[key] = value

        return self

    def cast(self, source_key: str, format_string: str, target_key: str = None) -> 'HarvestRecord':
        """
        Cast the value of a key to a different type.

        :param source_key: the name of the key
        :param format_string: the type to cast the value to
        :param target_key: when provided, a new key will be created with the cast value, defaults to None which overrides the existing key value.
        """

        from .functions import cast

        self[target_key or source_key] = cast(self[source_key], format_string)

        return self

    def clear_matches(self) -> 'HarvestRecord':
        """
        Removes all matches from the record.
        """

        self.non_matching_expressions.clear()
        self.matching_expressions.clear()

        return self

    def copy_key(self, source_key: str, target_key: str) -> 'HarvestRecord':
        """
        Copy the value of a key to a new key.

        :param source_key: the name of the source key
        :param target_key: the name of the target key
        """

        self[target_key] = self.get(source_key)

        return self

    def dict_from_json_string(self, source_key: str, operation: Literal['key', 'merge', 'replace'], new_key: str = None) -> 'HarvestRecord':
        """
        Convert a JSON string to a dictionary and perform an operation with it.

        :param source_key: the name of the key containing the JSON string
        :param operation: the operation to perform ('key', 'merge', or 'replace')
        When 'key', the JSON string is converted to a dictionary and stored in a new key.
        When 'merge', the JSON string is converted to a dictionary and merged with the existing record.
        When 'replace', the JSON string is converted to a dictionary and replaces the existing record.
        :param new_key: the name of the new key, defaults to None
        """

        from json import loads

        data = loads(self.get(source_key))

        match operation:
            case 'key':
                self[new_key] = data

            case 'merge':
                self.update(data)

            case 'replace':
                self[source_key] = data

        return self

    def first_not_null_value(self, *keys) -> 'HarvestRecord':
        """
        Get the first non-null value among a list of keys.

        :param keys: the keys to check
        :return: the first non-null value
        """

        for key in keys:
            if self.get(key):
                return self[key]

        return self

    def flatten(self, separator: str = '.') -> 'HarvestRecord':
        """
        Flatten the record.

        :param separator: the separator to use when flattening, defaults to '.'
        """

        if self.is_flat:
            return self

        from flatten_json import flatten
        flat = flatten(self, separator=separator)
        self.clear()
        self.update(flat)

        self.is_flat = True

        return self

    def key_value_list_to_dict(self,
                               source_key: str,
                               name_key: str = 'Key',
                               value_key: str = 'Value',
                               preserve_original: bool = False,
                               target_key: str = None) -> 'HarvestRecord':
        """
        Convert a list of key-value pairs to a dictionary.

        :param source_key: the name of the source key
        :param target_key: when provided, the result is placed in a new key, defaults to None
        :param name_key: the name of the key in the source list, defaults to 'Key'
        :param value_key: the name of the value in the source list, defaults to 'Value'
        :param preserve_original: whether to preserve the original key, defaults to False
        """

        from .functions import key_value_list_to_dict
        self[target_key or source_key] = key_value_list_to_dict(value=self[source_key],
                                                                key_name=name_key,
                                                                value_name=value_key)

        if not preserve_original and target_key and target_key != source_key:
            self.pop(source_key)

        return self

    def list_to_str(self, source_key: str, target_key: str = None, delimiter: str = '\n') -> 'HarvestRecord':
        """
        Convert a list to a string.

        :param source_key: the name of the source key
        :param target_key: the name of the target key, defaults to None
        :param delimiter: the delimiter to use when joining the elements, defaults to '\n' (newline)
        """

        self[target_key or source_key] = delimiter.join(self[source_key])

        return self

    def match(self) -> 'HarvestRecord':
        """
        Match the record against the match set.
        """

        self.matching_expressions, self.non_matching_expressions = self.recordset.match_set.match(self)

        return self

    def remove_key(self, key: str) -> 'HarvestRecord':
        """
        Remove a key from the record.

        :param key: the name of the key to remove
        """

        self.pop(key)

        return self

    def remove_keys_not_in(self, keys: List[str]) -> 'HarvestRecord':
        """
        Remove keys that are not in a list.

        :param keys: the list of keys to keep
        """

        [
            self.pop(key) for key in list(self.keys())
            if key not in keys
        ]

        return self

    def rename_key(self, old_key, new_key) -> 'HarvestRecord':
        """
        Rename a key in the record.

        :param old_key: the name of the old key
        :param new_key: the name of the new key
        """

        self[new_key] = self.pop(old_key)

        return self

    def reset_matches(self) -> 'HarvestRecord':
        """
        Reset the matches of the record.
        """

        self.matching_expressions.clear()
        self.non_matching_expressions.clear()

        return self

    def serialize(self, target_key: str, keep_other_keys: bool = False) -> 'HarvestRecord':
        """
        Converts the record into a serialized string.

        :param target_key: the name of the target key, defaults to None
        :param keep_other_keys: whether to keep the other keys in the record, defaults to False
        """

        from json import dumps
        serialized = {
            target_key: dumps(self)
        }

        if not keep_other_keys:
            self.clear()

        self.update(serialized)

        return self

    def split_key(self, source_key: str, target_key: str = None, delimiter: str = ' ') -> 'HarvestRecord':
        """
        Split the value of a key into a list.

        :param source_key: the name of the source key
        :param target_key: the name of the target key, defaults to None
        :param delimiter: the delimiter to use when splitting, defaults to ''
        """

        self[target_key or source_key] = self[source_key].split(delimiter) if isinstance(self[source_key], str) else self[source_key]

        return self

    def substring(self, source_key: str, start: int = None, end: int = None, target_key: str = None) -> 'HarvestRecord':
        """
        Get a substring of the value of a key.

        :param source_key: the name of the source key
        :param start: the start index of the substring
        :param end: the end index of the substring
        :param target_key: when provided, the result is placed in a new key, defaults to None
        """

        self[target_key or source_key] = self[source_key][start:end]

        return self

    def unflatten(self, separator: str = '.') -> 'HarvestRecord':
        """
        Unflatten the record.

        :param separator: the separator to use when unflattening, defaults to '.'
        """

        if self.is_flat is False:
            return self

        from flatten_json import unflatten_list
        unflat = unflatten_list(self, separator=separator)
        self.clear()
        self.update(unflat)

        self.is_flat = False

        return self


class HarvestRecordSet(List[HarvestRecord]):
    """
    A HarvestRecordSet is a list of HarvestRecord objects. It contains methods for performing operations on
    all records in the set.
    """

    def __init__(self, name: str = None, data: List[Dict] = None, **kwargs):
        """
        Initialize a HarvestRecordSet object.

        :param data: A list of dictionaries to initialize the record set with, defaults to None
        :param kwargs: Additional keyword arguments
        """

        super().__init__(**kwargs)

        from uuid import uuid4
        self.name = name or str(uuid4())

        self.indexes = {}
        self.index_fields = {}
        self.match_set = None

        if data:
            self.add(data=data)

    def __enter__(self) -> 'HarvestRecordSet':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def __add__(self, other):
        self.add(data=other)

        return self

    @property
    def keys(self) -> List[str]:
        """
        Retrieve a list of all unique keys for all records in the record set.
        """

        return sorted(list(set([key for record in self for key in record.keys()])))

    def add(self, data: (List[dict or HarvestRecord], ) or dict or HarvestRecord) -> 'HarvestRecordSet':
        """
        Add a list of records to the record set.

        This method accepts a list of dictionaries or HarvestRecord objects, a single dictionary, or a single HarvestRecord object.
        If the input is a dictionary, it will be automatically converted to a HarvestRecord.
        If the input is a HarvestRecord, it will be directly appended to the record set.
        If the input is a list, each element in the list will be appended to the record set. If an element is a dictionary, it will be converted to a HarvestRecord.

        After adding the new data, the indexes of the record set are rebuilt.

        Args:
            data (Union[List[Union[dict, 'HarvestRecord']], dict, 'HarvestRecord']): A list of dictionaries or HarvestRecord objects to add to the record set. If the object is a dictionary, it will automatically be converted to a HarvestRecord.

        Returns:
            HarvestRecordSet: The current HarvestRecordSet instance.
        """

        if isinstance(data, dict):
            self.append(HarvestRecord(recordset=self, **data))

        elif isinstance(data, HarvestRecord):
            data.recordset = self
            self.append(data)

        elif isinstance(data, list):
            [
                self.add(item)
                for item in data
            ]

        # Other data types (such as strings) can be added as a single item with the key 'item'
        else:
            self.add({'item': data})

        self.rebuild_indexes()

        return self

    def clear_matches(self) -> 'HarvestRecordSet':
        """
        Clear all matches from the record set.
        """

        [record.clear_matches() for record in self]

        return self

    def create_index(self, index_name: str, *fields) -> 'HarvestRecordSet':
        """
        Create an index for the record set.

        :param index_name: The name of the index
        :param fields: The fields to include in the index
        """

        index = {}
        for record in self:
            # Sometimes a dictionary may not have the associated field. In this case, we will use None as the value.
            key = tuple(record.get(field) for field in fields)

            if key in index:
                index[key].append(record)

            else:
                index[key] = [record]

        self.indexes[index_name] = index
        self.index_fields[index_name] = fields

        return self

    def drop_index(self, index_name: str) -> 'HarvestRecordSet':
        """
        Drop an index from the record set.

        :param index_name: The name of the index to drop
        """

        self.indexes.pop(index_name)

        return self

    def get_matched_records(self) -> 'HarvestRecordSet':
        """
        Get all records in the record set that are a match.

        :return: A HarvestRecordSet object containing all matched records
        """

        return HarvestRecordSet(data=[record for record in self if record.is_matched_record])

    def rebuild_indexes(self):
        """
        Rebuild all indexes for the record set.
        """

        self.indexes.clear()
        for index_name, fields in self.index_fields.items():
            self.create_index(index_name, *fields)

        return self

    def remove_duplicates(self) -> 'HarvestRecordSet':
        """
        Remove duplicate records from the record set.
        """

        unique_records = {
            frozenset(record.items()): record
            for record in self
        }

        self.clear()
        self.add(data=list(unique_records.values()))

        self.rebuild_indexes()

        return self

    def remove_unmatched_records(self) -> 'HarvestRecordSet':
        """
        Remove all records in the record set that are not a match.
        """

        self[:] = [
            record for record in self
            if record.is_matched_record
        ]

        self.rebuild_indexes()

        return self

    def set_match_set(self, syntax: str or List[str]) -> 'HarvestRecordSet':
        """
        Add a match to the record set.

        :param syntax: The match syntax to add
        """

        from .matching import HarvestMatchSet
        self.match_set = HarvestMatchSet(matches=syntax)

        return self

    def sort_records(self, keys: List[str]) -> 'HarvestRecordSet':
        """
        Sort the records in the record set by one or more keys.

        :param keys: The keys to sort by
        """

        sorted_keys = {}
        for s in keys:
            if ':' in s:
                key, value = s.split(':')
                key = key.strip()

                if value.lower() == 'desc':
                    order = -1
                else:
                    order = 1
            else:
                key = s
                order = 1

            sorted_keys[key] = order

        super().sort(key=lambda record: [
            (record[key] if sorted_keys[key] == 1 else -record[key])
            for key in sorted_keys
        ])



        return self

    def to_redis(self, key: str) -> 'HarvestRecordSet':
        """
        Convert the record set to a Redis-compatible format. This permanently modifies the record set.

        :param key: The key to store the record set under
        """
        from json import dumps

        result = [
            {
                record[key]: dumps(record)
                for key, value in record.items()
            }
            for record in self
        ]

        # Remove the original records
        self.clear()

        # Clear indexes
        self.indexes.clear()

        # Add the new records
        self.add(data=result)

        return self

    def unwind(self, source_key: str, preserve_null_and_empty_keys: bool = True) -> 'HarvestRecordSet':
        """
        Unwind a list of records in the record set into separate records.

        :param source_key: The key of the list to unwind
        :param preserve_null_and_empty_keys: Whether to preserve keys with null or empty values, defaults to True
        """

        new_records = []
        for record in self:
            if source_key not in record and preserve_null_and_empty_keys is False:
                continue

            elif isinstance(record[source_key], (list or tuple)):
                for item in record[source_key]:
                    new_record = record.copy()
                    new_record[source_key] = item
                    new_records.append(new_record)

            else:
                new_records.append(record)

        self.clear()
        self.add(data=new_records)

        return self


class HarvestRecordSets(Dict[str, HarvestRecordSet]):
    """
    A dictionary of HarvestRecordSet objects.
    """

    def add(self, recordset_name: str, recordset: HarvestRecordSet) -> 'HarvestRecordSets':
        """
        Add a record set to the HarvestRecordSets object.
        """

        self[recordset_name] = recordset

        return self

    def index(self, recordset_name: str, index_name: str, *fields) -> 'HarvestRecordSets':
        """
        Create an index for a record set.
        """

        self[recordset_name].create_index(index_name, fields)

        return self

    # TODO: implement join() of two or more HarvestRecordSets
    # def join(self, new_recordset_name: str, recordset_names: List[str], index_name: str,
    #          join_type: Literal['inner', 'outer', 'left', 'right']) -> 'HarvestRecordSets':
    #
    #     # Retrieve the recordsets
    #     recordsets = [self[recordset_name] for recordset_name in recordset_names]
    #
    #     # Retrieve the indexes
    #     indexes = [recordset.indexes[index_name] for recordset in recordsets if recordset.indexes.get(index_name)]
    #
    #     # Perform the join operation
    #     match join_type:
    #         case 'inner':
    #             joined_data = []
    #             for index in indexes:
    #                 joined_data.extend(index)
    #
    #
    #         case 'outer':
    #             joined_data = set.union(*map(set, indexes))
    #
    #         case 'left':
    #             joined_data = set(indexes[0]).union(*indexes[1:])
    #
    #         case 'right':
    #             joined_data = set(indexes[-1]).union(*indexes[:-1])
    #
    #         case _:
    #             raise ValueError('Invalid join type')
    #
    #     # Create a new recordset with the joined data
    #     new_recordset = HarvestRecordSet(data=joined_data)
    #
    #     # Add the new recordset to the dictionary
    #     self[new_recordset_name] = new_recordset
    #
    #     return self

    def list(self) -> List[dict]:
        """
        List all record sets in the HarvestRecordSets object.
        """

        return [
            {
                'Name': name,
                'Keys': '\n'.join(recordset.keys),
                'Matches': len(recordset.get_matched_records()),
                'Total': len(recordset)
            }
            for name, recordset in self.items()
        ]

    def purge(self) -> 'HarvestRecordSets':
        """
        Deletes all record sets from the HarvestRecordSets object.
        """

        self.clear()

        return self

    def query(self, recordset_name: str):
        """
        Retrieve a record set by name.
        """

        return self.get(recordset_name)

    def remove(self, name: str) -> 'HarvestRecordSets':
        """
        Remove a record set from the HarvestRecordSets object by name.
        """

        self.pop(name)
        return self

    def rename(self, old_recordset_name: str, new_recordset_name: str) -> 'HarvestRecordSets':
        """
        Changes the name of a record set in the HarvestRecordSets object.
        """

        self[new_recordset_name] = self.pop(old_recordset_name)

        return self

    def union(self, new_recordset_name: str, recordset_names: List[str]) -> 'HarvestRecordSets':
        """
        Combine two or more record sets into a new record set.
        """

        new_recordset = HarvestRecordSet()
        [new_recordset.add(data=self[recordset_name]) for recordset_name in recordset_names]

        self[new_recordset_name] = new_recordset

        return self

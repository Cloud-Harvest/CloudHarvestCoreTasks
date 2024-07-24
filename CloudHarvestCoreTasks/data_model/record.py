from typing import List, Literal
from collections import OrderedDict
from .matching import HarvestMatch


class HarvestRecord(OrderedDict):
    def __init__(self, recordset=False, is_flat: bool = False, **kwargs):
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

    def add_key_from_keys(self, new_key: str, sequence: List[str], delimiter: str = ' ', abort_on_none: bool = False) -> 'HarvestRecord':
        """
        This method creates a new key in the record, with its value being a concatenation of the values of the keys provided in the sequence.
        If a value in the sequence is not a key, it is interpreted as a literal string.

        :param new_key: The name of the new key to be added to the record.
        :param sequence: A list of keys whose values will be concatenated to form the value of the new key.
        :param delimiter: The delimiter to use when concatenating the values. Defaults to a space character.
        :param abort_on_none: If True, the method will abort if a value in the sequence is None. Defaults to False.
        :return: The record with the new key added.
        """

        result = []
        for s in sequence:
            if isinstance(s, dict):
                subresult = self.get(s.get('key'))

            else:
                subresult = s

            if abort_on_none and subresult is None:
                return self

            else:
                result.append(subresult)

        self[new_key] = delimiter.join([str(s) for s in result])

        return self

    def assign_elements_at_index_to_key(self, source_key: str, target_key: str, start: int = None, end: int = None, delimiter: str = None) -> 'HarvestRecord':
        """
        Assign elements at a specific index to a new key.

        :param source_key: the name of the source key
        :param target_key: the name of the target key
        :param start: the index start position
        :param end: the index end position
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
            return

        from flatten_json import flatten
        flat = flatten(self, separator=separator)
        self.clear()
        self.update(flat)

        self.is_flat = True

        return self

    def key_value_list_to_dict(self, source_key: str, name_key: str = 'Key',
                               value_key: str = 'Value', preserve_original: bool = False, target_key: str = None) -> 'HarvestRecord':
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

    def clear_matches(self) -> 'HarvestRecord':
        """
        Clear the matches of the record.
        """

        self.matching_expressions.clear()
        self.non_matching_expressions.clear()

        return self

    def match(self, syntax: str) -> bool:
        """
        Check if the record matches a statement.

        :param syntax: the match statement
        :return: True if the record matches the statement, False otherwise
        """

        match = HarvestMatch(record=self, syntax=syntax)
        match.match()

        if match.is_match:
            self.matching_expressions.append(match)

        else:
            self.non_matching_expressions.append(match)

        return match.is_match

    def remove_key(self, key: str) -> 'HarvestRecord':
        """
        Remove a key from the record.

        :param key: the name of the key to remove
        """

        self.pop(key)

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
            return

        from flatten_json import unflatten_list
        unflat = unflatten_list(self, separator=separator)
        self.clear()
        self.update(unflat)

        self.is_flat = False

        return self

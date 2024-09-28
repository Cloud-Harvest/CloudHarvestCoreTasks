"""
`matching.py`

This module provides classes and functions for performing matching operations on records using various syntaxes and
operators.

Note that, while we provide a MongoDb syntax, we do not offer an equivalent Redis syntax. This is because Redis does not
offer complex query filtering. Instead, follow the process in docs/tasks/redis.md which provides a means to retrieve
data and apply user-filters to the result.

This document includes the following key components:

Classes:
    - `HarvestMatch`: Represents a single matching operation on a record.
    - `HarvestMatchSet`: Represents a set of `HarvestMatch` instances and provides methods to convert them into MongoDB-compatible filters.

Functions:
    - `build_mongo_matching_syntax`: Converts a list of matching syntaxes into a MongoDB filter.

Key Concepts:
    - `_MATCH_OPERATIONS`: A dictionary mapping operators to their corresponding functions or methods.
    - `HarvestMatch`: Handles individual match conditions and converts them into MongoDB-compatible filters.
    - `HarvestMatchSet`: Handles multiple match conditions and combines them using `AND` or `OR` logic.

Usage:
    - Create instances of `HarvestMatch` or `HarvestMatchSet` with the desired matching syntax and record.
    - Use the `as_mongo_filter` method to convert the match conditions into MongoDB-compatible filters.
    - Use the `build_mongo_matching_syntax` function to convert a list of matching syntaxes into a MongoDB filter.

Example:
    ```python
    from collections import OrderedDict
    from matching import HarvestMatch, HarvestMatchSet

    record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
    matches = ['key1=value1', 'key2!=value2']

    # Create a HarvestMatchSet instance
    match_set = HarvestMatchSet(matches=matches, record=record)

    # Get MongoDB filter with AND logic
    and_filter = match_set.as_mongo_filter()
    print(and_filter)
    ```

Dependencies:
    - `operator`: Provides standard operators as functions.
    - `collections.OrderedDict`: Used for maintaining the order of keys in records.
    - `re.findall`: Used for regex matching.
    - `typing.List`: Used for type hinting.

"""

import operator
from collections import OrderedDict
from re import findall
from typing import List, Tuple

# The order of _MATCH_OPERATIONS's keys is important. The keys should be ordered from longest to shortest to ensure that
# the longest match is attempted first. For example, '==' should be before '=' to ensure that '==' is matched
# before '='. This allows us to perform split() operations on the syntax without accidentally splitting on a substring
# that is part of the operator.
_MATCH_OPERATIONS = {
        '==': operator.eq,  # Checks if 'a' is equal to 'b'
        '>=': operator.ge,  # Checks if 'a' is greater than or equal to 'b'
        '=>': operator.ge,  # Checks if 'a' is greater than or equal to 'b'
        '<=': operator.le,  # Checks if 'a' is less than or equal to 'b'
        '=<': operator.le,  # Checks if 'a' is less than or equal to 'b'
        '!=': operator.ne,  # Checks if 'a' is not equal to 'b'
        '>': operator.gt,   # Checks if 'a' is greater than 'b'
        '<': operator.lt,   # Checks if 'a' is less than 'b'
        '=': findall        # Checks if 'a' contains 'b'
    }


class HarvestMatch:
    """
    The HarvestMatch class is used to perform matching operations on a record based on a provided syntax.

    Attributes:
        syntax (str): The matching syntax to be used.
        key (str): The key to be used in the matching operation.
        value (str): The value to be used in the matching operation.
        operator (str): The operator to be used in the matching operation.
        final_match_operation (str): The final matching operation after processing.
        is_match (bool): The result of the matching operation.

    Methods:
        as_mongo_filter() -> dict:
            Converts the matching operation into a MongoDB match operation.

        match() -> bool:
            Performs the matching operation and returns the result.

        get_operator_key() -> str:
            Retrieves the operator key from the matching syntax.
    """

    def __init__(self, syntax: str):
        """
        Constructs a new HarvestMatch instance.

        Args:
            syntax (str): The matching syntax to be used.
        """

        self.syntax = syntax
        self.key = None
        self.value = None

        self.operator = self.get_operator_key()
        self.final_match_operation = None

    def as_mongo_filter(self) -> dict:
        """
        Converts the matching operation into a MongoDB match operation.

        Returns:
            dict: A dictionary representing the MongoDB match operation.
        """

        if self.key is None and self.value is None:
            self.key, self.value = self.syntax.split(self.operator, maxsplit=1)

            # strip whitespace from the key, value, and operator
            for v in ['key', 'value', 'operator']:
                if hasattr(getattr(self, v), 'strip'):
                    setattr(self, v, getattr(self, v).strip())

            # fuzzy cast the value to the appropriate type
            from .functions import fuzzy_cast
            self.value = fuzzy_cast(self.value)

            if self.value is None:
                return {
                    self.key: None
                }

        match self.operator:
            case '=':
                result = {
                    self.key: {
                        "$regex": str(self.value),
                        "$options": "i"
                    }
                }

            case '<=' | '=<':
                result = {
                    self.key: {
                        "$lte": self.value
                    }
                }

            case '>=' | '=>':
                result = {
                    self.key: {
                        "$gte": self.value
                    }
                }

            case '==':
                result = {
                    self.key: self.value
                }

            case '!=':
                result = {
                    self.key: {
                        "$ne": self.value
                    }
                }

            case '<':
                result = {
                    self.key: {
                        "$lt": self.value
                    }
                }

            case '>':
                result = {
                    self.key: {
                        "$gt": self.value
                    }
                }

            case _:
                raise ValueError('No valid matching statement returned')

        return result

    def as_sql_filter(self) -> tuple:
        """
        Converts the matching operation into an SQL WHERE clause condition.

        Returns:
            str: A string representing the SQL WHERE clause condition.
        """

        if self.key is None and self.value is None:
            self.key, self.value = self.syntax.split(self.operator, maxsplit=1)

            # strip whitespace from the key, value, and operator
            for v in ['key', 'value', 'operator']:
                if hasattr(getattr(self, v), 'strip'):
                    setattr(self, v, getattr(self, v).strip())

            # fuzzy cast the value to the appropriate type
            from .functions import fuzzy_cast
            self.value = fuzzy_cast(self.value)

        # Enclose string values in single quotes and self.operator is not '='
        value = f"'{self.value}'" if isinstance(self.value, str) and self.operator != '=' else self.value

        from uuid import uuid4
        key_uuid = str(uuid4()).replace('-', '')
        value_uuid = str(uuid4()).replace('-', '')

        param_key = f'%({key_uuid})s'
        param_value = f'%({value_uuid})s'

        match self.operator:
            case '=':
                result = f'{param_key} ILIKE "%{param_value}%"'

            case '<=' | '=<':
                result = f"{param_key} <= {param_value}"

            case '>=' | '=>':
                result = f"{param_key} >= {param_value}"

            case '==':
                result = f"{param_key} = {param_value}"

            case '!=':
                result = f"{param_key} != {param_value}"

            case '<':
                result = f"{param_key} < {param_value}"

            case '>':
                result = f"{param_key} > {param_value}"

            case _:
                raise ValueError('No valid matching statement returned')

        return result, {
            key_uuid: self.key,
            value_uuid: value
        }

    def match(self, record: OrderedDict) -> bool:
        """
        Performs the matching operation and returns the result.

        Arguments:
            record (OrderedDict): The record to be matched against.

        Returns:
            bool: The result of the matching operation.
        """

        self.key, self.value = self.syntax.split(self.operator, maxsplit=1)

        from .functions import is_bool, is_datetime, is_null, is_number
        matching_value = self.value
        record_key_value = record.get(self.key)

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

            from .functions import cast
            matching_value = cast(matching_value, cast_variables_as)
            record_key_value = cast(record_key_value, cast_variables_as)

        from re import findall, IGNORECASE
        if self.operator == '=':
            result = findall(pattern=matching_value, string=record_key_value, flags=IGNORECASE)

        else:
            result = _MATCH_OPERATIONS[self.operator](record_key_value, matching_value)

        self.final_match_operation = f'{record_key_value}{self.operator}{matching_value}'

        return result

    def get_operator_key(self):
        """
        Retrieves the operator key from the matching syntax.

        Returns:
            str: The operator key.

        Raises:
            ValueError: If no valid operator is found in the syntax.
        """

        for op in _MATCH_OPERATIONS.keys():
            if op in self.syntax:
                return op

        raise ValueError('No valid operator found in syntax. Valid operators are: ' + ', '.join(_MATCH_OPERATIONS.keys()))


class HarvestMatchSet(list):
    """
    The HarvestMatchSet class is a list of HarvestMatch instances. It is used to perform matching operations on a record
    based on a list of provided syntaxes.

    Attributes:
        matches (List[HarvestMatch]): The list of HarvestMatch instances.

    Methods:
        as_mongo_filter() -> dict:
            Converts the matching operations of all HarvestMatch instances into MongoDB match operations.
    """

    def __init__(self, matches: List[str]):
        """
        Constructs a new HarvestMatchSet instance.

        Args:
            matches (List[str]): The list of matching syntaxes to be used.
        """

        super().__init__()

        if isinstance(matches, str):
            self.matches = [HarvestMatch(syntax=matches)]

        elif isinstance(matches, list):
            self.matches = [HarvestMatch(syntax=match) for match in matches]

        else:
            raise ValueError('Invalid type for matches. Expected str or list of str, got ' + type(matches).__name__)

    def as_mongo_filter(self) -> dict:
        """
        Converts the matching operations of all HarvestMatch instances into MongoDB match operations.

        Returns:
            dict: A dictionary representing the MongoDB match operations.
        """

        result = {'$and': []}

        for match in self.matches:
            match_syntax = match.as_mongo_filter()
            result['$and'].append(match_syntax)

        # If there's only one match condition, simplify the result
        if len(result['$and']) == 1:
            result = result['$and'][0]

        return result

    def as_sql_filter(self) -> dict:
        """
        Converts the matching operations of all HarvestMatch instances into SQL WHERE clause conditions.

        Returns:
            dict: A dictionary representing the SQL WHERE clause conditions and parameters.
        """

        clauses = []
        parameters = {}

        for match in self.matches:
            match_syntax = match.as_sql_filter()
            clauses.append(match_syntax[0])
            parameters.update(match_syntax[1])

        # Combine conditions with the specified operator
        result = ' AND '.join(clauses)

        return {
            'clauses': result,
            'parameters': parameters
        }

    def match(self, record: OrderedDict) -> Tuple[List[str], List[str]]:
        """
        Performs the matching operation using all Matches in the MatchSet and returns the result. Multiple Matches are
        treated as an OR expression, meaning that if any of the Matches return True, the MatchSet will return True.

        Arguments:
            record (OrderedDict): The record to be matched against.

        Returns:
            bool: The result of the matching operation.
        """

        match_true = []
        match_false = []

        for match in self.matches:
            result = match.match(record)

            if result:
                match_true.append(match.final_match_operation)

            else:
                match_false.append(match.final_match_operation)

        return match_true, match_false

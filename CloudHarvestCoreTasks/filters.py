"""
This module accepts data filters provided by users and returns the derived syntax for various data destinations.
"""
from typing import Any, Generator, List, Literal
from logging import getLogger

logger = getLogger('harvest')

class BaseFilter:
    """
    Base class for user filters. This class is used to define the structure of the user filters. It is not intended
    for direct use; rather, it is intended to be inherited by the specific user filter classes.
    """

    # The default order of operations for all filters. This order is used to ensure that the filters are applied in an
    # optimal and consistent manner.
    ORDER_OF_OPERATIONS = (
        'add_keys',     # Need all possible keys for matching and sorting
        'matches',      # Filter the data
        'sort',         # Sort the data
        'limit',        # Limit the data
        'exclude_keys', # Exclude keys from the data
        'headers',      # Set the headers of the data
        'count'         # Return a count of the data
    )
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
            accepted (str, optional): A string containing the filter types to be applied. The default is None which applies no filters.
            add_keys (List[str], optional): The keys to be added to the data. Defaults to an empty list.
            count (str, optional): The count of the data. Defaults to None.
            exclude_keys (List[str], optional): The keys to be excluded from the data. Defaults to an empty list.
            headers (List[str], optional): The headers of the data. Defaults to an empty list.
            limit (int, optional): The limit of the data. Defaults to None.
            matches (List[List[str]], optional): The matches of the data. Defaults to an empty list.
            sort (List[str], optional): The sort of the data. Defaults to an empty list.
        """

        from re import compile
        self.accepted = compile(str(accepted)) if accepted else None

        # Initialize the filters
        self.add_keys = None
        self.count = None
        self.exclude_keys = None
        self.headers = None
        self.limit = None
        self.matches = None
        self.sort = None

        filter_defaults = {
            'add_keys': [],
            'count': False,
            'exclude_keys': [],
            'headers': [],
            'limit': None,
            'matches': [],  # Will be converted to the MatchSetGroup type
            'sort': None
        }

        for key, default in filter_defaults.items():
            setattr(self, key, self._if_accepted(key, locals().get(key), default))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _if_accepted(self, key: str, value: Any, default: Any) -> Any:
        """
        Returns the value if the filter is accepted; otherwise, returns the default value. When no 'accepted' expression
        is provided, the default is always returned.

        Arguments
        key (str): The key to be checked.
        value (Any): The value to be returned if the key is accepted.
        default (Any): The value to be returned if the key is not accepted.

        Returns
        Any: The value if the key is accepted; otherwise, the default value.
        """

        if self.accepted:
            return value if self.accepted.match(key) else default

        else:
            return default

    @property
    def result(self):
        return None

    def apply(self) -> 'BaseFilter':
        """
        This method applies the user filters to the data based on the ORDER_OF_OPERATIONS.
        """

        for operation in self.ORDER_OF_OPERATIONS:
            getattr(self, f'_{operation}')()

        return self

    def _add_keys(self, *args, **kwargs):
        pass

    def _count(self, *args, **kwargs):
        pass

    def _exclude_keys(self, *args, **kwargs):
        pass

    def _headers(self, *args, **kwargs):
        pass

    def _limit(self, *args, **kwargs):
        pass

    def _matches(self, *args, **kwargs):
        pass

    def _sort(self, *args, **kwargs):
        pass

    def keys(self) -> Generator[str, None, None]:
        """
        This method returns the expected keys of the data based on the provided headers, add_keys, and exclude_keys.

        Returns
        A generator of the headers of the data.
        """

        headers = self.headers if self.accepted.match('headers') else []
        add_keys = self.add_keys if self.accepted.match('add_keys') else []
        exclude_keys = self.exclude_keys if self.accepted.match('exclude_keys') else []

        for header in headers + add_keys:
            if header not in exclude_keys:
                yield header


class Match:
    from dataset import WalkableDict

    def __init__(self, syntax: str):
        self.syntax = syntax

        # Determine the operator for the match
        self.operator, \
        self.operator_method = self.get_operator()

        # Split the syntax into key, operator, and value
        self.key, \
        self.value = self.syntax.split(self.operator)

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

        match_operations = {
            '==': operator.eq,  # Checks if 'a' is equal to 'b'
            '>=': operator.ge,  # Checks if 'a' is greater than or equal to 'b'
            '=>': operator.ge,  # Checks if 'a' is greater than or equal to 'b'
            '<=': operator.le,  # Checks if 'a' is less than or equal to 'b'
            '=<': operator.le,  # Checks if 'a' is less than or equal to 'b'
            '!=': operator.ne,  # Checks if 'a' is not equal to 'b'
            '>': operator.gt,  # Checks if 'a' is greater than 'b'
            '<': operator.lt,  # Checks if 'a' is less than 'b'
            '=': findall  # Checks if 'a' contains 'b'
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
        Matches the item against the match.

        Arguments:
            item (WalkableDict): The item to be matched.

        Returns:
            bool: The result of the match.
        """

        from dataset import WalkableDict

        if not isinstance(item, WalkableDict):
            item = WalkableDict(item)

        from functions import is_bool, is_datetime, is_null, is_number
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

            from functions import cast
            matching_value = cast(matching_value, cast_variables_as)
            record_key_value = cast(record_key_value, cast_variables_as)

        from re import findall, IGNORECASE
        if self.operator == '=':
            result = findall(pattern=matching_value, string=record_key_value, flags=IGNORECASE)

        else:
            result = self.operator_method(record_key_value, matching_value)

        return result


class MatchSet(List[Match]):
    from dataset import WalkableDict

    def __init__(self, *args, **kwargs):
        super().__init__()

        for arg in args:
            if isinstance(arg, MatchSet):
                self.extend(arg)

            elif isinstance(arg, list):
                for a in arg:
                    if isinstance(a, Match):
                        self.append(a)

                    elif isinstance(a, str):
                        self.append(Match(a))


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
    from dataset import WalkableDict

    def __init__(self, *args: list or str or MatchSet, **kwargs):
        super().__init__()

        for arg in args:
            if isinstance(arg, MatchSetGroup):
                self.extend(arg)

            elif isinstance(arg, list):
                self.append(MatchSet(*arg))

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


class DataSetFilter(BaseFilter):
    """
    This class converts user filters to DataSet query syntax.
    """

    def __init__(self, dataset: List[dict] = None, *args, **kwargs):
        """
        Arguments:
            dataset (DataSet): The DataSet to be filtered.
        """

        super().__init__(*args, **kwargs)

        from dataset import DataSet
        self.dataset = dataset if isinstance(dataset, DataSet) else DataSet(dataset)

    @property
    def result(self):
        return self.dataset

    def _add_keys(self, *args, **kwargs) -> 'DataSetFilter':
        """
        This method returns the keys to be added to the data. If the keys already exist, their existing values are preserved.
        """
        if self.add_keys:
            self.dataset.add_keys(keys=self.add_keys, clobber=False)

        return self

    def _count(self, *args, **kwargs) -> int or None:
        """
        This method returns the count of the data.
        """

        if self.count:
            self.dataset = len(self.dataset)

    def _exclude_keys(self, *args, **kwargs) -> 'DataSetFilter':
        """
        This method returns the keys to be excluded from the data.
        """

        if self.exclude_keys:
            self.dataset.drop_keys(keys=self.exclude_keys)

        return self

    def _headers(self, *args, **kwargs) -> 'DataSetFilter':
        """
        This method returns the headers of the data.
        """
        self.dataset.set_keys(keys=list(self.keys()))

        return self

    def _limit(self, *args, **kwargs) -> 'DataSetFilter':
        """
        This method returns the limit of the data.
        """
        if self.limit:
            self.dataset.limit(self.limit)

        return self

    def _matches(self, *args, **kwargs):
        """
        This method returns the matches of the data.
        """

        if self.matches:
            self.dataset.match_and_remove(matching_expressions=self.matches)

        return self

    def _sort(self) -> 'DataSetFilter':
        """
        This method returns the sort of the data based on the provided sort or keys(). When keys() is used, each key is
        sorted in the default sort order (ascending).
        """

        self.dataset.sort_records(keys=self.sort or self.keys())

        return self

    def apply(self) -> 'DataSetFilter':
        super().apply()

        return self


class MongoFilter(BaseFilter):
    """
    This class converts user filters to MongoDB query syntax.
    """

    ORDER_OF_OPERATIONS = (
        'add_keys',     # Need all possible keys for matching and sorting
        'matches',      # Filter the data
        'sort',         # Sort the data
        'limit',        # Limit the data
        'project',      # Project the data
        'count'         # Return a count of the data
    )

    def __init__(self, pipeline: list = None, *args, **kwargs):
        """
        Arguments:
            pipeline (list, optional) -- A list of MongoDB pipeline stages. Defaults to an empty list. The pipeline
            is only required when the MongoDB uses the pipeline stage syntax, such as the 'aggregate' method. When not
            provided, the output will be a dictionary of simpler 'find'-like query syntax.
        """

        super().__init__(*args, **kwargs)

        self.pipeline = pipeline or []

    def apply(self) -> 'MongoFilter':
        """
        This method converts user filters to MongoDB pipeline query syntax based on the documentation here:
        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        """

        result = []

        for operation in self.ORDER_OF_OPERATIONS:
            result.append(getattr(self, f'_{operation}')())

        return self

    def _add_keys(self, *args, **kwargs) -> dict or None:
        if self.add_keys:
            return {
                '$addFields': {
                    key: f'${key}'
                    for key in self.add_keys
                }
            }

    def _count(self, *args, **kwargs) -> dict or None:
        if self.count:
            return {'$count': self.count}

    def _exclude_keys(self, *args, **kwargs) -> dict or None:
        # MongoDb does not have a built-in excludeKeys method. Instead, we use the $project stage to exclude keys.
        return

    def _limit(self, *args, **kwargs) -> dict or None:
        if self.limit:
            return {'$limit': self.limit}

    def _matches(self) -> dict or None:
        """
        Converts matching syntax into a MongoDb filter.
        """
        def convert_match_to_language(match: Match) -> dict:
            """
            Converts a single match into a MongoDB filter.

            Arguments:
                match (Match): A single group of matches (MatchSet) to be converted into a MongoDB filter.
            """

            if match.key is None and match.value is None:
                match.key, match.value = match.syntax.split(match.operator, maxsplit=1)

                # strip whitespace from the key, value, and operator
                for v in ['key', 'value', 'operator']:
                    if hasattr(getattr(match, v), 'strip'):
                        setattr(match, v, getattr(match, v).strip())

                # fuzzy cast the value to the appropriate type
                from functions import fuzzy_cast
                match.value = fuzzy_cast(match.value)

                if match.value is None:
                    return {
                        match.key: None
                    }

            match match.operator:
                case '=':
                    result = {
                        match.key: {
                            "$regex": str(match.value),
                            "$options": "i"
                        }
                    }

                case '<=' | '=<':
                    result = {
                        match.key: {
                            "$lte": match.value
                        }
                    }

                case '>=' | '=>':
                    result = {
                        match.key: {
                            "$gte": match.value
                        }
                    }

                case '==':
                    result = {
                        match.key: match.value
                    }

                case '!=':
                    result = {
                        match.key: {
                            "$ne": match.value
                        }
                    }

                case '<':
                    result = {
                        match.key: {
                            "$lt": match.value
                        }
                    }

                case '>':
                    result = {
                        match.key: {
                            "$gt": match.value
                        }
                    }

                case _:
                    raise ValueError('No valid matching statement returned')

            return result

        matches_results = []

        for match_set in self.matches:
            # Declare the AND filter for the match set
            match_set_and = {}

            # Convert each match in the match set to a MongoDB filter
            [
                match_set_and.update(convert_match_to_language(match))
                for match in match_set
            ]

            # Append the AND filter to the list of match results
            matches_results.append(match_set_and)

        match len(matches_results):
            case 0:
                # No matches
                return None

            case 1:
                # If there is only one match, return it directly
                return matches_results[0]

            case _:
                # If there are multiple matches, each item in matches_results is a different declaration of the OR filter
                # https://www.mongodb.com/docs/manual/reference/operator/aggregation/or/

                return {
                    '$or': matches_results
                }

    def _project(self, *args, **kwargs) -> dict or None:
        return {
            '$project': {
                key: 1
                for key in self.keys()
            }
        }

    def _sort(self, *args, **kwargs) -> dict or None:
        if self.sort:
            result = {}

            for sort in self.sort:
                if ':' in sort:
                    field, direction = sort.split(':')
                    result[field] = direction

                else:
                    result[sort] = 1

            return result


class SqlFilter(BaseFilter):
    """
    This class converts user filters to SQL query syntax.
    """

    ORDER_OF_OPERATIONS = (
        'select',       # Select the data
        'from',         # From the data
        'matches',      # Filter the data (WHERE clause)
        'sort',         # Sort the data
        'limit'         # Limit the data
    )
    def __init__(self, sql: str, object_quoting: Literal['single', 'double', 'backtick', 'none'] = 'double', *args, **kwargs):
        """
        Arguments:
            table_definitions (List[str]): The table_definitions to retrieve data from.
            object_quoting (Literal['single', 'double', 'backtick', 'none'], optional): The type of quoting to be used for SQL objects. Defaults to 'double'.
        """
        super().__init__(*args, **kwargs)

        self.sql = sql

        # Set the object quoting character. In databases, object quoting is used to enclose object names, such as table
        # names and column names, in case they contain special characters or are reserved words. The quoting character
        # is used to enclose the object name in the SQL query. For example, in MySQL, the backtick (`) is used to enclose
        # object names while PostgreSQL uses double quotes (") are used.
        match object_quoting:
            case 'single':
                self.object_quoting = "'"

            case 'double':
                self.object_quoting = '"'

            case 'backtick':
                self.object_quoting = '`'

            case _:
                self.object_quoting = ''

        self.sql = ''

    def _add_keys(self):
        # SQL does not have a built-in addKeys method. Instead, we use the SELECT statement to add keys.
        pass

    def _count(self):
        # Due to the nature of how SQL works, we need to wrap the entire query in a subquery to get the count. Therefore,
        # the count operation is handled in the apply() method.
        pass

    def _exclude_keys(self):
        # SQL does not have a built-in excludeKeys method. Instead, we use the SELECT statement to exclude keys.
        pass

    def _limit(self) -> str or None:
        if self.limit:
            return f'LIMIT {self.limit}'

    def _matches(self, match: Match) -> tuple:
        """
        Converts the matching operations into an SQL WHERE clause condition.

        Returns:
            str: A string representing the SQL WHERE clause condition.
        """

        if match.key is None and match.value is None:
            match.key, match.value = match.syntax.split(match.operator, maxsplit=1)

            # strip whitespace from the key, value, and operator
            for v in ['key', 'value', 'operator']:
                if hasattr(getattr(match, v), 'strip'):
                    setattr(match, v, getattr(match, v).strip())

            # fuzzy cast the value to the appropriate type
            from functions import fuzzy_cast
            match.value = fuzzy_cast(match.value)

        # Enclose string values in single quotes and match.operator is not '='
        value = f"'{match.value}'" if isinstance(match.value, str) and match.operator != '=' else match.value

        from uuid import uuid4
        key_uuid = str(uuid4()).replace('-', '')
        value_uuid = str(uuid4()).replace('-', '')

        param_key = f'%({key_uuid})s'
        param_value = f'%({value_uuid})s'

        match match.operator:
            case '=':
                result = f"{param_key} ILIKE '%{param_value}%'"

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
            key_uuid: match.key,
            value_uuid: value
        }

    def _sort(self) -> str:
        result = []

        for sort in self.sort:
            if ':' in sort:
                field, direction = sort.split(':')
                result.append(f'{self.object_quoting}{field}{self.object_quoting} direction')

            else:
                result.append(f'{self.object_quoting}{sort}{self.object_quoting} ASC')

        return f'ORDER BY {", ".join(result)}'

    def _select(self) -> str:
        keys = list(self.keys())
        if keys:
            fields = ', '.join([
                f'{self.object_quoting}{key}{self.object_quoting}'
                for key in keys
            ])

        else:
            fields = '*'

        return f'SELECT {fields}'

    def _from(self) -> str:
        return f'({self.sql}) as pre_filter_sql'

    @property
    def result(self):
        return self.sql

    def apply(self) -> 'SqlFilter':
        """
        This method converts user filters to SQL query syntax based on the ORDER_OF_OPERATIONS.
        """

        result = []

        for operation in self.ORDER_OF_OPERATIONS:
            result.append(getattr(self, f'_{operation}')())

        self.sql = ' '.join(result)

        # In SQL, the count operation requires a subquery. Therefore, we need to wrap the entire query in a subquery
        # to get the count. This is done here to ensure that the count is the last operation performed.
        if self.count:
            self.sql = f'SELECT COUNT(*) FROM ({self.sql}) AS result_count'

        return self

# """
# This module accepts data filters provided by users and returns the derived syntax for various data destinations.
# """
# from typing import Any, Generator, List, Literal
# from logging import getLogger
#
# logger = getLogger('harvest')
#
# class BaseFilter:
#     """
#     Base class for user filters. This class is used to define the structure of the user filters. It is not intended
#     for direct use; rather, it is intended to be inherited by the specific user filter classes.
#     """
#
#     # The default order of operations for all filters. This order is used to ensure that the filters are applied in an
#     # optimal and consistent manner.
#     ORDER_OF_OPERATIONS = (
#         'add_keys',     # Need all possible keys for matching and sorting
#         'matches',      # Filter the data
#         'sort',         # Sort the data
#         'limit',        # Limit the data
#         'exclude_keys', # Exclude keys from the data
#         'headers',      # Set the headers of the data
#         'count'         # Return a count of the data
#     )
#     def __init__(self,
#                  accepted: str = None,
#                  add_keys: List[str] = None,
#                  count: str = None,
#                  exclude_keys: List[str] = None,
#                  headers: List[str] = None,
#                  limit: int = None,
#                  matches: List[List[str]] = None,
#                  sort: List[str] = None,
#                  *arg, **kwargs):
#
#         """
#         Arguments:
#             accepted (str, optional): A string containing the filter types to be applied. The default is None which applies no filters.
#             add_keys (List[str], optional): The keys to be added to the data. Defaults to an empty list.
#             count (str, optional): The count of the data. Defaults to None.
#             exclude_keys (List[str], optional): The keys to be excluded from the data. Defaults to an empty list.
#             headers (List[str], optional): The headers of the data. Defaults to an empty list.
#             limit (int, optional): The limit of the data. Defaults to None.
#             matches (List[List[str]], optional): The matches of the data. Defaults to an empty list.
#             sort (List[str], optional): The sort of the data. Defaults to an empty list.
#         """
#
#         from re import compile
#         self.accepted = compile(str(accepted)) if accepted else None
#
#         # Initialize the filters
#         self.add_keys = None
#         self.count = None
#         self.exclude_keys = None
#         self.headers = None
#         self.limit = None
#         self.matches = None
#         self.sort = None
#
#         filter_defaults = {
#             'add_keys': [],
#             'count': False,
#             'exclude_keys': [],
#             'headers': [],
#             'limit': None,
#             'matches': [],  # Will be converted to the MatchSetGroup type
#             'sort': None
#         }
#
#         for key, default in filter_defaults.items():
#             setattr(self, key, self._if_accepted(key, locals().get(key), default))
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         pass
#
#     def _if_accepted(self, key: str, value: Any, default: Any) -> Any:
#         """
#         Returns the value if the filter is accepted; otherwise, returns the default value. When no 'accepted' expression
#         is provided, the default is always returned.
#
#         Arguments
#         key (str): The key to be checked.
#         value (Any): The value to be returned if the key is accepted.
#         default (Any): The value to be returned if the key is not accepted.
#
#         Returns
#         Any: The value if the key is accepted; otherwise, the default value.
#         """
#
#         if self.accepted:
#             return value if self.accepted.match(key) else default
#
#         else:
#             return default
#
#     @property
#     def result(self):
#         return None
#
#     def apply(self) -> 'BaseFilter':
#         """
#         This method applies the user filters to the data based on the ORDER_OF_OPERATIONS.
#         """
#
#         for operation in self.ORDER_OF_OPERATIONS:
#             getattr(self, f'_{operation}')()
#
#         return self
#
#     def _add_keys(self, *args, **kwargs):
#         pass
#
#     def _count(self, *args, **kwargs):
#         pass
#
#     def _exclude_keys(self, *args, **kwargs):
#         pass
#
#     def _headers(self, *args, **kwargs):
#         pass
#
#     def _limit(self, *args, **kwargs):
#         pass
#
#     def _matches(self, *args, **kwargs):
#         pass
#
#     def _sort(self, *args, **kwargs):
#         pass
#
#     def keys(self) -> List[str]:
#         """
#         This method returns the expected keys of the data based on the provided headers, add_keys, and exclude_keys.
#
#         Returns
#         A list of the headers of the data.
#         """
#
#         headers = self.headers or [] if self.accepted.match('headers') else []
#         add_keys = self.add_keys or [] if self.accepted.match('add_keys') else []
#         exclude_keys = self.exclude_keys or [] if self.accepted.match('exclude_keys') else []
#
#         return [
#             header for header in (headers + add_keys)
#             if header not in exclude_keys
#         ]
#
#
#
#
#
# # class DataSetFilter(BaseFilter):
# #     """
# #     This class converts user filters to DataSet query syntax.
# #     """
# #
# #     def __init__(self, dataset: List[dict] = None, *args, **kwargs):
# #         """
# #         Arguments:
# #             dataset (DataSet): The DataSet to be filtered.
# #         """
# #
# #         super().__init__(*args, **kwargs)
# #
# #         from CloudHarvestCoreTasks.dataset import DataSet
# #         if dataset:
# #             if isinstance(dataset, DataSet):
# #                 self.dataset = dataset
# #             else:
# #                 self.dataset = DataSet(dataset)
# #
# #     @property
# #     def result(self):
# #         return self.dataset
# #
# #     def _add_keys(self, *args, **kwargs) -> 'DataSetFilter':
# #         """
# #         This method returns the keys to be added to the data. If the keys already exist, their existing values are preserved.
# #         """
# #         if self.add_keys:
# #             self.dataset.add_keys(keys=self.add_keys, clobber=False)
# #
# #         return self
# #
# #     def _count(self, *args, **kwargs) -> int or None:
# #         """
# #         This method returns the count of the data.
# #         """
# #
# #         if self.count:
# #             self.dataset = len(self.dataset)
# #
# #     def _exclude_keys(self, *args, **kwargs) -> 'DataSetFilter':
# #         """
# #         This method returns the keys to be excluded from the data.
# #         """
# #
# #         if self.exclude_keys:
# #             self.dataset.drop_keys(keys=self.exclude_keys)
# #
# #         return self
# #
# #     def _headers(self, *args, **kwargs) -> 'DataSetFilter':
# #         """
# #         This method returns the headers of the data.
# #         """
# #         self.dataset.set_keys(keys=list(self.keys()))
# #
# #         return self
# #
# #     def _limit(self, *args, **kwargs) -> 'DataSetFilter':
# #         """
# #         This method returns the limit of the data.
# #         """
# #         if self.limit:
# #             self.dataset.limit(self.limit)
# #
# #         return self
# #
# #     def _matches(self, *args, **kwargs):
# #         """
# #         This method returns the matches of the data.
# #         """
# #
# #         if self.matches:
# #             self.dataset.match_and_remove(matching_expressions=self.matches)
# #
# #         return self
# #
# #     def _sort(self) -> 'DataSetFilter':
# #         """
# #         This method returns the sort of the data based on the provided sort or keys(). When keys() is used, each key is
# #         sorted in the default sort order (ascending).
# #         """
# #
# #         self.dataset.sort_records(keys=self.sort or self.keys())
# #
# #         return self
# #
# #     def apply(self) -> 'DataSetFilter':
# #         super().apply()
# #
# #         return self
#
#
# class MongoFilter(BaseFilter):
#     """
#     This class converts user filters to MongoDB query syntax.
#     """
#
#     ORDER_OF_OPERATIONS = (
#         'add_keys',     # Need all possible keys for matching and sorting
#         'matches',      # Filter the data
#         'sort',         # Sort the data
#         'limit',        # Limit the data
#         'project',      # Project the data
#         'count'         # Return a count of the data
#     )
#
#     def __init__(self, pipeline: list = None, *args, **kwargs):
#         """
#         Arguments:
#             pipeline (list, optional) -- A list of MongoDB pipeline stages. Defaults to an empty list. The pipeline
#             is only required when the MongoDB uses the pipeline stage syntax, such as the 'aggregate' method. When not
#             provided, the output will be a dictionary of simpler 'find'-like query syntax.
#         """
#
#         super().__init__(*args, **kwargs)
#
#         self.pipeline = pipeline or []
#
#     def apply(self) -> 'MongoFilter':
#         """
#         This method converts user filters to MongoDB pipeline query syntax based on the documentation here:
#         https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
#         """
#
#         result = []
#
#         for operation in self.ORDER_OF_OPERATIONS:
#             result.append(getattr(self, f'_{operation}')())
#
#         return self
#
#     def _add_keys(self, *args, **kwargs) -> dict or None:
#         if self.add_keys:
#             return {
#                 '$addFields': {
#                     key: f'${key}'
#                     for key in self.add_keys
#                 }
#             }
#
#     def _count(self, *args, **kwargs) -> dict or None:
#         if self.count:
#             return {'$count': self.count}
#
#     def _exclude_keys(self, *args, **kwargs) -> dict or None:
#         # MongoDb does not have a built-in excludeKeys method. Instead, we use the $project stage to exclude keys.
#         return
#
#     def _limit(self, *args, **kwargs) -> dict or None:
#         if self.limit:
#             return {'$limit': self.limit}
#
#     def _matches(self) -> dict or None:
#         """
#         Converts matching syntax into a MongoDb filter.
#         """
#         def convert_match_to_language(match: Match) -> dict:
#             """
#             Converts a single match into a MongoDB filter.
#
#             Arguments:
#                 match (Match): A single group of matches (MatchSet) to be converted into a MongoDB filter.
#             """
#
#             if match.key is None and match.value is None:
#                 match.key, match.value = match.syntax.split(match.operator, maxsplit=1)
#
#                 # strip whitespace from the key, value, and operator
#                 for v in ['key', 'value', 'operator']:
#                     if hasattr(getattr(match, v), 'strip'):
#                         setattr(match, v, getattr(match, v).strip())
#
#                 # fuzzy cast the value to the appropriate type
#                 from CloudHarvestCoreTasks.functions import fuzzy_cast
#                 match.value = fuzzy_cast(match.value)
#
#                 if match.value is None:
#                     return {
#                         match.key: None
#                     }
#
#             match match.operator:
#                 case '=':
#                     result = {
#                         match.key: {
#                             "$regex": str(match.value),
#                             "$options": "i"
#                         }
#                     }
#
#                 case '<=' | '=<':
#                     result = {
#                         match.key: {
#                             "$lte": match.value
#                         }
#                     }
#
#                 case '>=' | '=>':
#                     result = {
#                         match.key: {
#                             "$gte": match.value
#                         }
#                     }
#
#                 case '==':
#                     result = {
#                         match.key: match.value
#                     }
#
#                 case '!=':
#                     result = {
#                         match.key: {
#                             "$ne": match.value
#                         }
#                     }
#
#                 case '<':
#                     result = {
#                         match.key: {
#                             "$lt": match.value
#                         }
#                     }
#
#                 case '>':
#                     result = {
#                         match.key: {
#                             "$gt": match.value
#                         }
#                     }
#
#                 case _:
#                     raise ValueError('No valid matching statement returned')
#
#             return result
#
#         matches_results = []
#
#         for match_set in self.matches:
#             # Declare the AND filter for the match set
#             match_set_and = {}
#
#             # Convert each match in the match set to a MongoDB filter
#             [
#                 match_set_and.update(convert_match_to_language(match))
#                 for match in match_set
#             ]
#
#             # Append the AND filter to the list of match results
#             matches_results.append(match_set_and)
#
#         match len(matches_results):
#             case 0:
#                 # No matches
#                 return None
#
#             case 1:
#                 # If there is only one match, return it directly
#                 return matches_results[0]
#
#             case _:
#                 # If there are multiple matches, each item in matches_results is a different declaration of the OR filter
#                 # https://www.mongodb.com/docs/manual/reference/operator/aggregation/or/
#
#                 return {
#                     '$or': matches_results
#                 }
#
#     def _project(self, *args, **kwargs) -> dict or None:
#         return {
#             '$project': {
#                 key: 1
#                 for key in self.keys()
#             }
#         }
#
#     def _sort(self, *args, **kwargs) -> dict or None:
#         if self.sort:
#             result = {}
#
#             for sort in self.sort:
#                 if ':' in sort:
#                     field, direction = sort.split(':')
#                     result[field] = direction
#
#                 else:
#                     result[sort] = 1
#
#             return result
#
#
# class SqlFilter(BaseFilter):
#     """
#     This class converts user filters to SQL query syntax.
#     """
#
#     ORDER_OF_OPERATIONS = (
#         'select',       # Select the data
#         'from',         # From the data
#         'matches',      # Filter the data (WHERE clause)
#         'sort',         # Sort the data
#         'limit'         # Limit the data
#     )
#     def __init__(self, sql: str, object_quoting: Literal['single', 'double', 'backtick', 'none'] = 'double', *args, **kwargs):
#         """
#         Arguments:
#             table_definitions (List[str]): The table_definitions to retrieve data from.
#             object_quoting (Literal['single', 'double', 'backtick', 'none'], optional): The type of quoting to be used for SQL objects. Defaults to 'double'.
#         """
#         super().__init__(*args, **kwargs)
#
#         self.sql = sql
#
#         # Set the object quoting character. In databases, object quoting is used to enclose object names, such as table
#         # names and column names, in case they contain special characters or are reserved words. The quoting character
#         # is used to enclose the object name in the SQL query. For example, in MySQL, the backtick (`) is used to enclose
#         # object names while PostgreSQL uses double quotes (") are used.
#         match object_quoting:
#             case 'single':
#                 self.object_quoting = "'"
#
#             case 'double':
#                 self.object_quoting = '"'
#
#             case 'backtick':
#                 self.object_quoting = '`'
#
#             case _:
#                 self.object_quoting = ''
#
#         self.sql = ''
#
#     def _add_keys(self):
#         # SQL does not have a built-in addKeys method. Instead, we use the SELECT statement to add keys.
#         pass
#
#     def _count(self):
#         # Due to the nature of how SQL works, we need to wrap the entire query in a subquery to get the count. Therefore,
#         # the count operation is handled in the apply() method.
#         pass
#
#     def _exclude_keys(self):
#         # SQL does not have a built-in excludeKeys method. Instead, we use the SELECT statement to exclude keys.
#         pass
#
#     def _limit(self) -> str or None:
#         if self.limit:
#             return f'LIMIT {self.limit}'
#
#     def _matches(self, match: Match) -> tuple:
#         """
#         Converts the matching operations into an SQL WHERE clause condition.
#
#         Returns:
#             str: A string representing the SQL WHERE clause condition.
#         """
#
#         if match.key is None and match.value is None:
#             match.key, match.value = match.syntax.split(match.operator, maxsplit=1)
#
#             # strip whitespace from the key, value, and operator
#             for v in ['key', 'value', 'operator']:
#                 if hasattr(getattr(match, v), 'strip'):
#                     setattr(match, v, getattr(match, v).strip())
#
#             # fuzzy cast the value to the appropriate type
#             from CloudHarvestCoreTasks.functions import fuzzy_cast
#             match.value = fuzzy_cast(match.value)
#
#         # Enclose string values in single quotes and match.operator is not '='
#         value = f"'{match.value}'" if isinstance(match.value, str) and match.operator != '=' else match.value
#
#         from uuid import uuid4
#         key_uuid = str(uuid4()).replace('-', '')
#         value_uuid = str(uuid4()).replace('-', '')
#
#         param_key = f'%({key_uuid})s'
#         param_value = f'%({value_uuid})s'
#
#         match match.operator:
#             case '=':
#                 result = f"{param_key} ILIKE '%{param_value}%'"
#
#             case '<=' | '=<':
#                 result = f"{param_key} <= {param_value}"
#
#             case '>=' | '=>':
#                 result = f"{param_key} >= {param_value}"
#
#             case '==':
#                 result = f"{param_key} = {param_value}"
#
#             case '!=':
#                 result = f"{param_key} != {param_value}"
#
#             case '<':
#                 result = f"{param_key} < {param_value}"
#
#             case '>':
#                 result = f"{param_key} > {param_value}"
#
#             case _:
#                 raise ValueError('No valid matching statement returned')
#
#         return result, {
#             key_uuid: match.key,
#             value_uuid: value
#         }
#
#     def _sort(self) -> str:
#         result = []
#
#         for sort in self.sort:
#             if ':' in sort:
#                 field, direction = sort.split(':')
#                 result.append(f'{self.object_quoting}{field}{self.object_quoting} direction')
#
#             else:
#                 result.append(f'{self.object_quoting}{sort}{self.object_quoting} ASC')
#
#         return f'ORDER BY {", ".join(result)}'
#
#     def _select(self) -> str:
#         keys = list(self.keys())
#         if keys:
#             fields = ', '.join([
#                 f'{self.object_quoting}{key}{self.object_quoting}'
#                 for key in keys
#             ])
#
#         else:
#             fields = '*'
#
#         return f'SELECT {fields}'
#
#     def _from(self) -> str:
#         return f'({self.sql}) as pre_filter_sql'
#
#     @property
#     def result(self):
#         return self.sql
#
#     def apply(self) -> 'SqlFilter':
#         """
#         This method converts user filters to SQL query syntax based on the ORDER_OF_OPERATIONS.
#         """
#
#         result = []
#
#         for operation in self.ORDER_OF_OPERATIONS:
#             result.append(getattr(self, f'_{operation}')())
#
#         self.sql = ' '.join(result)
#
#         # In SQL, the count operation requires a subquery. Therefore, we need to wrap the entire query in a subquery
#         # to get the count. This is done here to ensure that the count is the last operation performed.
#         if self.count:
#             self.sql = f'SELECT COUNT(*) FROM ({self.sql}) AS result_count'
#
#         return self

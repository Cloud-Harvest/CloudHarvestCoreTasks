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

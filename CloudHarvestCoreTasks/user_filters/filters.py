"""
This module accepts data filters provided by users and returns the derived syntax for various data destinations.
"""
from typing import List, Literal
from logging import getLogger

from .base import BaseUserFilter

logger = getLogger('harvest')


class MongoUserFilter(BaseUserFilter):
    """
    This class converts user filters to MongoDB query syntax.
    """

    def __init__(self, pipeline: list = None, *args, **kwargs):
        """
        Arguments:
            pipeline (list, optional) -- A list of MongoDB pipeline stages. Defaults to an empty list. The pipeline
            is only required when the MongoDB uses the pipeline stage syntax, such as the 'aggregate' method. When not
            provided, the output will be a dictionary of simpler 'find'-like query syntax.
        """

        super().__init__(*args, **kwargs)

        self.pipeline = pipeline or []
        self.pre_syntax = None

    def apply(self) -> 'MongoUserFilter':
        """
        This method converts user filters to MongoDB query syntax.
        """

        pre_syntax = super().apply().pre_syntax

        if self.pipeline:
            self.result = self._aggregate(pre_syntax)

        else:
            self.result = self._find(pre_syntax)

        return self

    def _aggregate(self, pre_syntax: dict) -> 'MongoUserFilter':
        """
        This method converts user filters to MongoDB pipeline query syntax based on the documentation here:
        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.aggregate

        Arguments:
            pre_syntax (dict): The pre-converted syntax from the user filters.
        """

        from copy import copy
        pipeline = copy(self.pipeline)

        # As a quirk of development, the BaseUserFilter.convert() returns sort in the same pattern as is used by MongoDb.
        add_keys = {'$addFields': {key: f'${key}' for key in pre_syntax['add_keys']}} if pre_syntax['add_keys'] else None
        count = {'$count': pre_syntax['count']} if pre_syntax['count'] else None
        limit = {'$limit': pre_syntax['limit']} if pre_syntax['limit'] else None
        matches = self._matches(pre_syntax['matches']) if pre_syntax['matches'] else None
        project = {'$project': {key: 1 for key in pre_syntax['headers']}} if pre_syntax['headers'] else None
        sort = {'$sort': pre_syntax['sort']} if pre_syntax['sort'] else None

        stage_order = (add_keys, matches, project, sort, limit, count)

        [
            pipeline.append(stage)
            for stage in stage_order
            if stage
        ]

        return self

    def _find(self, pre_syntax: dict) -> 'MongoUserFilter':
        """
        This method converts user filters to MongoDB find query syntax based on the documentation here:
        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.find

        Arguments:
            pre_syntax (dict): The pre-converted syntax from the user filters.
        """
        projection = {key: 1 for key in pre_syntax['headers']} if pre_syntax['headers'] else None

        # As a quirk of development, the BaseUserFilter.convert() returns sort in the same pattern as is used by MongoDb.
        arguments = {
            'projection': projection,
            'filter': self._matches(pre_syntax['matches']) or None,
            'limit': pre_syntax['limit'] or None,
            'sort': pre_syntax['sort'] or None
        }

        self.result = arguments

        return self

    def _matches(self, pre_syntax: dict) -> dict:
        """
        Converts matching syntax into a MongoDb filter.
        """
        matches = pre_syntax['matches']

        from data_model.matching import HarvestMatchSet
        # Convert the matches into HarvestMatchSet instances
        if len(matches) == 1:
            result = HarvestMatchSet(matches=matches[0]).as_mongo_filter()

        # If there are multiple matches, convert each match into a HarvestMatchSet instance and combine them into a single
        # MongoDB filter. When combining multiple matches, the matches are combined with an OR operation.
        else:
            result = {
                '$or': [
                    HarvestMatchSet(matches=match).as_mongo_filter()
                    for match in matches
                ]
            }

        return result


class HarvestRecordSetUserFilter(BaseUserFilter):
    """
    This class converts user filters to HarvestRecordSet query syntax.
    """

    from data_model.recordset import HarvestRecordSet
    def __init__(self, recordset: HarvestRecordSet = None, *args, **kwargs):
        """
        Arguments:
            recordset (HarvestRecordSet): The HarvestRecordSet to be filtered.
        """

        super().__init__(*args, **kwargs)

        self.recordset = recordset

    def apply(self):
        recordset = self.recordset

        pre_syntax = super().apply().pre_syntax

        headers = pre_syntax['headers']

        # Set matching
        if pre_syntax['matches']:
            # Create the match set
            recordset.set_match_set(pre_syntax['matches'])

            # Get the matched records, removing any records that do not match
            recordset = recordset.get_matched_records()

        # Set the limit
        if pre_syntax['limit']:
            recordset = recordset[:pre_syntax['limit']]

        # Set the count
        if pre_syntax['count']:
            result = {
                pre_syntax['count']: len(self.recordset)
            }

        else:
            # Set the headers
            result = recordset.modify_records(function='remove_keys_not_in', arguments={'keys': headers})

            # Sort the records
            if pre_syntax['sort']:
                result = result.sort_records(pre_syntax['sort'])

        return result


class SqlUserFilters(BaseUserFilter):
    """
    This class converts user filters to SQL query syntax.
    """

    def __init__(self, sql: str = None, object_quoting: Literal['single', 'double', 'backtick', 'none'] = 'double', *args, **kwargs):
        """
        Arguments:
            sql (str): The SQL query to be executed.
            object_quoting (Literal['single', 'double', 'backtick', 'none'], optional): The type of quoting to be used for SQL objects. Defaults to 'double'.
        """
        super().__init__(*args, **kwargs)

        self.sql = sql

        # Set the object quoting character
        match object_quoting:
            case 'single':
                self.object_quoting = "'"
            case 'double':
                self.object_quoting = '"'
            case 'backtick':
                self.object_quoting = '`'
            case _:
                self.object_quoting = ''

    def apply(self) -> dict:
        pre_syntax = super().apply().pre_syntax

        # Generate the SELECT clause
        _select, _select_params = self._select(pre_syntax['headers']) if pre_syntax['headers'] else ('SELECT *', {})

        # 'from' is a Python keyword
        _from = f'FROM ({self.sql}) AS {self.object_quoting}result{self.object_quoting}'

        # Generate the WHERE clause
        _where, _where_params = self._where(pre_syntax['matches']) if pre_syntax['matches'] else ('', {})

        # Set the limit
        _limit, _limit_params = self._limit(pre_syntax['limit']) if pre_syntax['limit'] else ('', {})

        # Set the sort
        _sort = 'ORDER BY ' + ', '.join([f'{key} {value}' for key, value in pre_syntax['sort'].items()]) if pre_syntax['sort'] else ''

        sql = f'{_select} {_from} {_where} {_sort} {_limit}'

        # Count clause
        if pre_syntax['count']:
            sql = 'SELECT COUNT(*) FROM (' + sql + ') AS result_count'

        return {
            'sql': sql,
            'parameters': {**_select_params, **_where_params, **_limit_params}
        }

    def _select(self, headers) -> tuple:
        from uuid import uuid4

        parameterized_headers = {
            str(uuid4()).replace('-', '_'): header for header in headers
        }

        result = 'SELECT ' + ', '.join([key for key in parameterized_headers.keys()])

        return result, parameterized_headers

    def _where(self, matches: List[List[str]]) -> tuple:
        """
        Converts matching syntax into an SQL WHERE clause condition.

        Args:
            matches (List[List[str]]): A list of lists of matching syntaxes.

        Returns:
            tuple: A tuple containing the SQL WHERE clause condition and parameters.
        """
        from data_model.matching import HarvestMatchSet
        # Convert the matches into HarvestMatchSet instances
        clauses = []
        parameters = {}

        for match in matches:
            clause, parameters = HarvestMatchSet(matches=match).as_sql_filter()
            clauses.append(clause)
            parameters.update(parameters)

        # Combine the clauses
        clauses = ' OR '.join(f'({clauses})')

        return 'WHERE ' + clauses, parameters

    def _order(self, sort: dict) -> tuple:
        from uuid import uuid4

        # Convert the sort into a parameterized dictionary
        sort_parameters = {
            str(uuid4()).replace('-', '_'): (key, 'DESC' if value == -1 else 'ASC')
            for key, value in sort.items()
        }

        # Generate the ORDER BY clause using the uuid and ASC/DESC values
        syntax = 'ORDER BY ' + ', '.join([f'%({key}) {value[1]}' for key, value in sort_parameters.values()])

        # Return the syntax, and the parameters using uuid and key names
        return syntax, {key: value[0] for key, value in sort_parameters.items()}

    def _limit(self, limit: int) -> tuple:
        from uuid import uuid4
        uid = str(uuid4()).replace('-', '_')

        return f'LIMIT %({uid})s', {uid: limit}

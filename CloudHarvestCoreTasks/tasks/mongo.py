from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseDataTask, BaseFilterableTask
from CloudHarvestCoreTasks.exceptions import TaskException

from pymongo import MongoClient


@register_definition(name='mongo', category='task')
class MongoTask(BaseDataTask, BaseFilterableTask):
    """
    The MongoTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a MongoDB database.
    """

    def __init__(self, collection: str = None, result_attribute: str = None, *args, **kwargs):
        """
        Initializes a new instance of the MongoTask class.

        Args:
            collection (str, optional): The name of the collection to interact with. When not provided, database-level commands are exposed.
            result_attribute (str, optional): The attribute to retrieve from the result.
            *args: Variable length argument list passed to the parent class.
            **kwargs: Arbitrary keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)

        # Make sure the default pipeline is a list
        if self.command == 'aggregate' and not self.arguments.get('pipeline'):
            self.arguments['pipeline'] = []

        self.collection = collection
        self.result_attribute = result_attribute

        self.order_of_operations = (
            'matches',      # Filter the data
            'add_keys',     # Need all possible keys for matching and sorting
            'sort',         # Sort the data
            'project',      # Project the data
            'limit',        # Limit the data
            'count'         # Return a count of the data
        )

    def apply_filters(self) -> 'MongoTask':
        """
        Applies user filters to the Task. The default user filter class is HarvestRecordSetUserFilter which is executed
        when on_complete() is called. This method should be overwritten in subclasses to provide specific functionality.
        """

        # If the user filters not configured for this Task, return
        if self.filters is None or self.ignore_filters:
            pass

        else:
            """
            This method converts user filters to MongoDB pipeline query syntax based on the documentation here:
            https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
            """

            result = []

            for operation in self.order_of_operations:
                pipeline_stage = getattr(self, f'_filter_{operation}')()

                if pipeline_stage is not None:
                    result.append(pipeline_stage)

            if self.arguments.get('pipeline') is None:
                self.arguments['pipeline'] = result

            else:
                if not isinstance(self.arguments['pipeline'], list):
                    self.arguments['pipeline'] = []

                self.arguments['pipeline'].extend(result)

        return self

    def _filter_add_keys(self, *args, **kwargs) -> dict or None:
        """
        This method returns the keys to be added to the data. If the keys already exist, their existing values are preserved.
        """
        if self.add_keys:
            return {
                '$addFields': {
                    key: {
                        '$ifNull': [f'${key}', None]
                    }
                    for key in self.add_keys
                }
            }

    def _filter_count(self, *args, **kwargs) -> dict or None:
        """
        This method returns the count of the data.
        """

        if self.count:
            return {'$count': self.count}

    def _filter_exclude_keys(self, *args, **kwargs) -> dict or None:
        # MongoDb does not have a built-in excludeKeys method. Instead, we use the $project stage to exclude keys.
        return

    def _filter_headers(self, *args, **kwargs) -> 'MongoTask':
        """
        This method returns the headers of the data.
        """
        self.data.set_keys(keys=list(self.filter_keys()))

        return self

    def _filter_limit(self, *args, **kwargs) -> dict or None:
        """
        This method returns the limit of the data.
        """
        if self.limit:
            return {'$limit': self.limit}

    def _filter_matches(self, *args, **kwargs) -> dict or None:
        """
        Converts matching syntax into a MongoDb filter.
        """
        from CloudHarvestCoreTasks.dataset import MatchSetGroup, MatchSet, Match

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
                from CloudHarvestCoreTasks.functions import fuzzy_cast
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

        # Convert the matches to a MatchSetGroup
        self.matches = MatchSetGroup(self.matches)

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
                return {
                    '$match': matches_results[0]
                }

            case _:
                # If there are multiple matches, each item in matches_results is a different declaration of the OR filter
                # https://www.mongodb.com/docs/manual/reference/operator/aggregation/or/

                return {
                    '$match':{
                        '$or': matches_results
                    }
                }

    def _filter_project(self, *args, **kwargs) -> dict or None:
        """
        This method returns a Mongo projection of the desired keys. This stage is unique to Mongo.
        """
        return {
            '$project': {
                key: 1
                for key in self.filter_keys()
            }
        }

    def _filter_sort(self) -> dict or None:
        if self.sort:
            result = {}

            for sort in self.sort:
                if ':' in sort:
                    field, direction = sort.split(':')
                    result[field] = direction

                else:
                    result[sort] = 1

            return {
                '$sort': result
            }

    def method(self, *args, **kwargs):
        """
        Runs the task. This method will execute the method defined in `self.command` on the database or collection and
        store the result in the result attribute. `self.result_attribute` is used to extract the desired attribute
        from the result, if applicable.
        """
        if self.filters and self.command != 'aggregate':
            raise TaskException(self, 'Filters are only supported for the aggregate command.')

        else:
            self.apply_filters()

        client: MongoClient = self.silo.connect()

        if not self.silo.is_connected:
            raise TaskException(self, f'Unable to connect to the {self.silo.name} silo.')

        if self.collection:
            # Note that MongoDb does not return an error if a collection is not found. Instead, MongoDb will faithfully
            # create the new collection name, even if it malformed or incorrect. This is an intentional feature of MongoDb.
            database_object = client[self.silo.database][self.collection]

        else:
            # Expose database-level commands
            database_object = client[self.silo.database]

        # Execute the command on the database or collection
        self.calls += 1

        result = self.walk_result_command_path(
            getattr(database_object, self.base_command_part)(**self.arguments)
        )

        # Convert the result to a list if it is a generator or cursor
        from types import GeneratorType
        from pymongo import CursorType
        from pymongo.cursor import Cursor
        from pymongo.command_cursor import CommandCursor
        from CloudHarvestCoreTasks.dataset import DataSet

        if isinstance(result, (GeneratorType, CommandCursor, CursorType, Cursor)):
            result = DataSet([
                document for document in result
            ])

        # Record the result
        self.result = result

        return self

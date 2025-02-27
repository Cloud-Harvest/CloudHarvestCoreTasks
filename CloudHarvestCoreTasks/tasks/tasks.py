"""
tasks.py - This module contains classes for various tasks that can be used in task chains.

A Task must be registered with the CloudHarvestCorePluginManager in order to be used in a task chain. This is done by
decorating the Task class with the @register_definition decorator. The name of the task is specified in the decorator
and is used to reference the task in task chain configurations.

Tasks are subclasses of the BaseTask class, which provides common functionality for all tasks. Each task must implement
a method() function that performs the task's operation. The method() function should return the task instance.
"""

from CloudHarvestCorePluginManager.decorators import register_definition
from logging import getLogger
from typing import Any, List, Literal

from pymongo import MongoClient

from .base import (
    BaseDataTask,
    BaseTask,
    BaseTaskChain,
    TaskChainException,
    TaskException,
    TaskStatusCodes
)

from ..filters import MongoFilter

logger = getLogger('harvest')

@register_definition(name='dummy', category='task')
class DummyTask(BaseTask):
    """
    The DummyTask class is a subclass of the Base
    Task class. It represents a task that does nothing when run. Used for testing.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes a new instance of the DummyTask class.
        """
        super().__init__(*args, **kwargs)

    def method(self) -> 'DummyTask':
        """
        This method does nothing. It is used to represent a task that does nothing when run.

        Returns:
            DummyTask: The current instance of the DummyTask class.
        """
        self.result = [{'dummy': 'data'}]
        self.meta = {'info': 'this is dummy metadata'}

        return self


@register_definition(name='error', category='task')
class ErrorTask(BaseTask):
    """
    The ErrorTask class is a subclass of the BaseTask class. It represents a task that raises an exception when run.
    This task is used for testing error handling in task chains and should not be used in production code. For example,
    this task is used for testing the `on: error` directive in task chain configurations.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def method(self):
        raise TaskException(self, 'This is an error task')


@register_definition(name='file', category='task')
class FileTask(BaseTask):
    """
    The FileTask class is a subclass of the BaseTask class. It represents a task that performs file operations such as
    reading from or writing to a file. Read operations take the content of a file and places them in the TaskChain's
    variables. Write operations take the data from the TaskChain's variables and write them to a file.

    Methods:
        determine_format(): Determines the format of the file based on its extension.
        method(): Performs the file operation specified by the mode and format attributes.
    """

    def __init__(self,
                 path: str,
                 mode: Literal['append', 'read', 'write'],
                 desired_keys: List[str] = None,
                 format: Literal['config', 'csv', 'json', 'raw', 'yaml'] = None,
                 template: str = None,
                 *args, **kwargs):

        """
        Initializes a new instance of the FileTask class.

        Args:
            path (str): The path to the file.
            mode (Literal['append', 'read', 'write']): The mode in which the file will be opened.
            desired_keys (List[str], optional): A list of keys to filter the data by. Defaults to None.
            format (Literal['config', 'csv', 'json', 'raw', 'yaml'], optional): The format of the file. Defaults to None.
            template (str, optional): A template to use for the output. Defaults to None.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """

        super().__init__(*args, **kwargs)
        from pathlib import Path

        # Class-specific
        self.path = path
        self.abs_path = Path(self.path).expanduser().absolute()
        self.mode = mode.lower()
        self.format = str(format or self.determine_format()).lower()
        self.desired_keys = desired_keys
        self.template = template

        # Value Checks
        if self.mode == 'read':
            if self.result_as is None:
                raise TaskException(self, f'The `result_as` attribute is required for read operations.')

    def determine_format(self):
        """
        Determines the format of the file based on its extension.

        Returns:
            str: The format of the file.
        """

        supported_extensions_and_formats = {
            'cfg': 'config',
            'conf': 'config',
            'config': 'config',
            'csv': 'csv',
            'ini': 'config',
            'json': 'json',
            'yaml': 'yaml',
            'yml': 'yaml'
        }

        path_file_extensions = str(self.path.split('.')[-1]).lower()

        return supported_extensions_and_formats.get(path_file_extensions) or 'raw'

    def method(self):
        """
        Performs the file operation specified by the mode and format attributes.

        This method handles both reading from and writing to files in various formats such as config, csv, json, raw, and yaml.

        Raises:
            BaseTaskException: If the data format is not supported for the specified operation.

        Returns:
            self: Returns the instance of the FileTask.
        """
        from configparser import ConfigParser
        from csv import DictReader, DictWriter
        import json
        import yaml

        modes = {
            'append': 'a',
            'read': 'r',
            'write': 'w'
        }

        # Open the file in the specified mode
        with open(self.abs_path, modes[self.mode]) as file:

            # Read operations
            if self.mode == 'read':
                if self.format == 'config':
                    config = ConfigParser()
                    config.read_file(file)
                    result = {section: dict(config[section]) for section in config.sections()}

                elif self.format == 'csv':
                    result = list(DictReader(file))

                elif self.format == 'json':
                    result = json.load(file)

                elif self.format == 'yaml':
                    result = yaml.load(file, Loader=yaml.FullLoader)

                else:
                    result = file.read()

                # If desired_keys is specified, filter the result to just those keys
                if self.desired_keys:
                    if isinstance(result, dict):
                        self.result = {k: v for k, v in result.items() if k in self.desired_keys}

                    elif isinstance(result, list):
                        self.result = [{k: v for k, v in record.items() if k in self.desired_keys} for record in result]

                # Return the entire result
                else:
                    self.result = result

            # Write operations
            else:

                try:
                    # If the user has specified desired_keys, filter the data to just those keys, if applicable
                    if self.desired_keys:
                        from flatten_json import flatten, unflatten
                        separator = '.'

                        # If the data is a dictionary, flatten it, filter the keys, and unflatten it
                        if isinstance(self.data, dict):
                            self.data = unflatten({
                                k: v for k, v in flatten(self.data.items, separator=separator)()
                                if k in self.desired_keys
                            }, separator=separator)

                        # If the data is a list, flatten each record, filter the keys, and unflatten each record
                        elif isinstance(self.data, list):
                            self.data = [
                                unflatten({
                                    k: v for k, v in flatten(record, separator=separator).items()
                                    if k in self.desired_keys
                                })
                                for record in self.data
                            ]

                    if self.format == 'config':
                        config = ConfigParser()
                        if isinstance(self.data, dict):
                            config.read_dict(self.data)

                            config.write(file)

                        else:
                            raise TaskException(self, f'`FileTask` only supports dictionaries for writes to config files.')

                    elif self.format == 'csv':
                        if isinstance(self.data, list):
                            if all([isinstance(record, dict) for record in self.data]):
                                consolidated_keys = set([key for record in self.data for key in record.keys()])
                                use_keys = self.desired_keys or consolidated_keys

                                writer = DictWriter(file, fieldnames=use_keys)
                                writer.writeheader()

                                writer.writerows(self.data)

                                return self

                        raise TaskException(self, f'`FileTask` only supports lists of dictionaries for writes to CSV files.')

                    elif self.format == 'json':
                        json.dump(self.data, file, default=str, indent=4)

                    elif self.format == 'yaml':
                        yaml.dump(self.data, file)

                    else:
                        file.write(str(self.data))
                finally:
                    from os.path import exists
                    if not exists(self.abs_path):
                        raise TaskException(self, f'The file `{file}` was not written to disk.')

        return self


@register_definition(name='dataset', category='task')
class DataSetTask(BaseTask):
    """
    The HarvestRecordSetTask class is a subclass of the BaseTask class. It represents a task that operates on a record set.

    Attributes:
        data (Any): The record set to operate on.
        stages: A list of dictionaries containing the function name and arguments to be applied to the recordset.
        >>> stages = [
        >>>     {
        >>>         'function_name': {
        >>>             'argument1': 'value1',
        >>>             'argument2': 'value2'
        >>>         }
        >>>     }
        >>> ]

    Methods:
        method(): Executes the function on the record set with the provided arguments and stores the result in the data attribute.
    """

    def __init__(self, stages: List[dict], data: Any = None, *args, **kwargs):
        """
        Constructs a new HarvestRecordSetTask instance.

        Args:
            stages (List[dict]): A list of dictionaries containing the function name and arguments to be applied to the recordset.
            data (Any, optional): The record set to operate on. Defaults to None.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        from ..dataset import DataSet
        self.data = data if isinstance(data, DataSet) else DataSet().add_records(data)
        self.stages = stages
        self.stage_position = 0

        from filters import DataSetFilter
        self.filters = DataSetFilter(self.filters)

    def apply_filters(self) -> 'DataSetTask':
        """
        Applies user filters to the Task. The default user filter class is HarvestRecordSetUserFilter which is executed
        when on_complete() is called. This method should be overwritten in subclasses to provide specific functionality.
        """

        # If the user filters not configured for this Task, return
        if self.filters.accepted is None or self.ignore_filters:
            pass

        else:
            self.filters.apply()

        return self

    def method(self):
        """
        Executes functions on the recordset with the provided function and arguments, then stores the result in the data attribute.

        This method iterates over the `stages` defined for this task. For each stage, it retrieves the function and its arguments.
        It then checks if the function is a method of the HarvestRecordSet or HarvestRecord class. If it is, it applies the function to the record set or each record in the record set, respectively.
        If the function is not a method of either class, it raises an AttributeError.

        The result of applying the function is stored in the data attribute of the HarvestRecordSetTask instance.

        Returns:
            self: Returns the instance of the HarvestRecordSetTask.
        """

        from ..dataset import DataSet

        for stage in self.stages:
            try:
                # Each dictionary should only contain one key-value pair
                for function, arguments in stage.items():

                    # This is a HarvestRecordSet command
                    if hasattr(DataSet, function):
                        # We don't template RecordSet commands because they are not intended to be used with record-level data
                        getattr(self.data, function)(**(arguments or {}))

                    else:
                        raise TaskException(self, f"Command '{function}' does not exist for the DataSetTask.")

            except Exception as ex:
                raise TaskException(self, f"Error executing stage [{self.stages.index(stage) + 1}] {list(stage.keys())[0]}: {str(ex)}")

            # Increment the stage_position
            self.stage_position += 1

        self.result = self.data

        return self


@register_definition(name='prune', category='task')
class PruneTask(BaseTask):
    def __init__(self, previous_task_data: bool = False, stored_variables: bool = False, *args, **kwargs):
        """
        Prunes the task chain.

        This method can be used to clear the data of previous tasks and/or the stored variables in the task chain.
        This can be useful to free up memory during the execution of a long task chain.

        Args:
            previous_task_data (bool, optional): If True, the data of all previous tasks in the task chain will be cleared. Defaults to False.
            stored_variables (bool, optional): If True, all variables stored in the task chain will be cleared. Defaults to False.

        Returns:
            BaseTaskChain: The current instance of the task chain.
        """

        super().__init__(*args, **kwargs)
        self.previous_task_data = previous_task_data
        self.stored_variables = stored_variables

    def method(self) -> 'PruneTask':
        from sys import getsizeof
        total_bytes_pruned = 0

        # If previous_task_data is True, clear the data of all previous tasks
        if self.previous_task_data:
            for i in range(self.task_chain.position):
                if str(self.task_chain[i].status) in [TaskStatusCodes.complete, TaskStatusCodes.error, TaskStatusCodes.skipped]:
                    total_bytes_pruned += getsizeof(self.task_chain[i].result)
                    self.task_chain[i].result = None

        # If stored_variables is True, clear all variables stored in the task chain
        if self.stored_variables:
            total_bytes_pruned += getsizeof(self.task_chain.variables)
            self.task_chain.variables.clear()

        self.result = {
            'total_bytes_pruned': total_bytes_pruned
        }

        return self

@register_definition(name='harvest_update', category='task')
class HarvestUpdateTask(BaseTask):
    """
    The HarvestTask class is a subclass of the MongoTask class. It represents a task that performs the steps necessary
    to upload data collected in a BaseHarvestTaskChain to a MongoDB database.
    """

    REQUIRED_METADATA_FIELDS = (
        'Platform',                     # The Platform (ie AWS, Azure, Google)
        'Service',                      # The Platform's service name (ie RDS, EC2, GCP)
        'Type',                         # The Service subtype, if applicable (ie RDS instance, EC2 event)
        'Account',                      # The Platform account name or identifier
        'Region',                       # The geographic region name for the Platform
        'UniqueIdentifierKeys',             # UniqueIdentifierKeys requires at least one value, so .0 is expected
        'Module.Author',                # The author of the Harvest module
        'Module.Name',                  # The name of the Harvest module that collected the data
        'Module.Url',                   # The repository where the Harvest module is stored
        'Module.Version',               # The version of the Harvest module
        'Dates.DeactivatedOn',          # The date the record was deactivated, if applicable
        'Dates.LastSeen',               # The date indicating when the record was last collected by Harvest
        'Active'                        # A boolean indicating if the record is active
    )

    def __init__(self, *args, **kwargs):
        """
        Initializes a new instance of the HarvestRecordUpdateTask class. This class is used to update records in the
        destination silo and the metadata silo with the data collected in the BaseHarvestTaskChain. This task is
        automatically added to the end of the task chain by the BaseHarvestTaskChain class.

        HarvestRecordUpdateTask accepts no arguments. They are instead supplied by the BaseHarvestTaskChain. Indeed,
        the HarvestRecordUpdateTask exists to separate the functionality of updating records from the BaseHarvestTaskChain
        so that metrics for the update process can be collected separately from the data collection.

        Although not an argument, the HarvestRecordUpdateTask requires that it have a parent task chain that is an
        instance of BaseHarvestTaskChain. This is enforced in the __init__ method.
        """

        super().__init__(*args, **kwargs)

        # Ensure that the task chain is a BaseHarvestTaskChain
        from .base import BaseHarvestTaskChain
        if not isinstance(self.task_chain, BaseHarvestTaskChain):
            raise TaskException(self, 'HarvestTask must be used in a BaseHarvestTaskChain.')

        # Type hint for the task_chain attribute
        from typing import cast
        from .base import BaseHarvestTaskChain
        self.task_chain = cast(BaseHarvestTaskChain, self.task_chain)

    def method(self) -> 'HarvestUpdateTask':
        """
        Executes the task.

        Returns:
            HarvestUpdateTask: The current instance of the HarvestTask class.
        """

        self.meta['Stages'] = []

        # Validate the Task can reach the required silos
        from ..silos import get_silo
        for silo_name in (self.task_chain.destination_silo, 'harvest-core'):
            try:
                get_silo(silo_name).connect().server_info()

            except Exception as ex:
                raise TaskException(self, f'Unable to connect to the {silo_name} silo. {str(ex)}')

        # Attach metadata to the records
        data = self.attach_metadata_to_records(data=self.data, metadata=self.build_metadata())

        # Bulk Replace the records in the destination silo and the metadata in the metadata silo
        unique_filters = self.replace_bulk_records(data=data)

        # Deactivate records that were not found in this data collection operation on the destination silo and the metadata silo
        deactivation_results = self.deactivate_records(unique_filters=unique_filters)

        self.result = {
            'RecordsProcessed': len(data),
            'RecordsReplaced': len(unique_filters),
            'DeactivationResults': deactivation_results
        }

        return self

    @staticmethod
    def attach_metadata_to_records(data: List[dict], metadata: dict) -> List[dict]:
        """
        This method attaches metadata to the records in the data list. It also generates the UniqueIdentifier for each record.

        Arguments:
            data (List[dict]): The list of records to attach metadata to.
            metadata (dict): The metadata to attach to the records.
        """

        for record in data:
            # Generate this record's unique filter
            from functions import get_nested_values
            unique_identifier = '-'.join([get_nested_values(s=field, d=record)[0] for field in metadata['UniqueIdentifierKeys']])

            # Attach existing metadata to the record
            record.update({'Harvest': metadata | {'UniqueIdentifier': unique_identifier}})

        return data

    def build_metadata(self) -> dict:
        """
        This method generates metadata for the task chain based on the class attributes and the task chain's metadata.
        """

        # PSTAR data
        pstar = {
            'Platform': self.task_chain.platform,
            'Service': self.task_chain.service,
            'Type': self.task_chain.type,
            'Account': self.task_chain.account,
            'Region': self.task_chain.region,
            'UniqueIdentifierKeys': self.task_chain.unique_identifier_keys,
            'Active': True  # Active by default because records found in this collection process are known to exist
        }

        # Convert the class / module metadata into a dictionary with Titled keys
        # As of CloudHarvestCorePluginManager 0.1.5, class metadata is recorded when the @register_definition
        # decorator is called, allowing the dynamic recording of metadata for each registered Harvest module and class.
        build_components = {
            'Module': {
                str(k).title(): v
                for k, v in getattr(self, '_harvest_plugin_metadata', {}).items()}
        }

        from datetime import datetime, timezone
        dates = {
            'Dates': {
                'DeactivatedOn': None,
                'LastSeen': datetime.now(tz=timezone.utc).isoformat()
            }
        }

        # Records Silo information
        silo = {
            'Silo': {
                'Name': self.task_chain.destination_silo,
                'Collection': self.task_chain.replacement_collection_name
             }
        }

        # Merge the components into a single metadata dictionary
        result = pstar | build_components | dates | silo

        # Validate that all required metadata fields are present
        from functions import get_nested_values
        missing_fields = [
            field for field in self.REQUIRED_METADATA_FIELDS
            if not get_nested_values(s=field, d=result)
        ]

        if missing_fields:
            raise TaskException(self, f'Missing required metadata fields: {missing_fields}')

        else:
            return result

    def replace_bulk_records(self, data: List[dict]) -> list:
        """
        This method Replaces a list of records into the specified silo.

        Args:
            data (List[dict]): The list of records to Replace.

        Returns:
            list: The list of unique filters for the records that were processed.
        """
        replacements = []
        metadata =[]

        from datetime import datetime, timezone
        from pymongo import ReplaceOne
        from ..silos import get_silo

        for record in data:
            # Remove an existing MongoDb _id field if it exists. This happens if the data source is MongoDB. We don't
            # want to set the _id field because it is the primary key in MongoDB which should not be overwritten by this process.
            from bson import ObjectId
            if isinstance(record.get('_id'), ObjectId):
                record.pop('_id')

            replace_filter = {'Harvest.UniqueIdentifier': record['Harvest']['UniqueIdentifier']}

            replace_resource = ReplaceOne(filter=replace_filter,
                                          replacement=record,
                                          upsert=True)

            # Gather the extra metadata fields for the record
            from functions import get_nested_values
            extras = {
                field: get_nested_values(s=field, d=record)
                for field in self.task_chain.extra_metadata_fields
            }

            replace_meta = ReplaceOne(filter=replace_filter,
                                      replacement=record['Harvest'] | {'Tags': record.get('Tags') or {}} | extras,
                                      upsert=True)

            replacements.append(replace_resource)
            metadata.append(replace_meta)

        def bulk_replace(silo_name: str, collection: str, prepared_replacements: List[ReplaceOne]) -> dict:
            """
            This method performs a bulk Replace operation on the specified silo.

            Args:
                silo_name (str): The name of the silo where the records will be Replaced.
                collection (str): The name of the collection where the records will be Replaced.
                prepared_replacements (List[ReplaceOne]): The list of Replace operations to perform.

            Returns:
                dict: The result of the Replace operation.
            """
            start_time = datetime.now(tz=timezone.utc)

            silo = get_silo(silo_name)
            client = silo.connect()

            bulk_replace_results = client[silo.database][collection].bulk_write(requests=prepared_replacements)

            end_time = datetime.now(tz=timezone.utc)

            return {
                'StartTime': start_time,
                'BulkReplaceResults': bulk_replace_results,
                'EndTime': end_time,
            }

        if replacements:
            # Perform the bulk Replace operations
            replacement_results = bulk_replace(silo_name=self.task_chain.destination_silo,
                                               collection=self.task_chain.replacement_collection_name,
                                               prepared_replacements=replacements)

            self.meta['Stages'].append({'BulkReplaceDocuments': replacement_results})


        if metadata:
            metadata_results = bulk_replace(silo_name='harvest-core',
                                            collection='metadata',
                                            prepared_replacements=metadata)

            # Store the results in the metadata
            self.meta['Stages'].append({'BulkReplaceMetadata': metadata_results})

        # Gather ObjectId's of all the records that were processed based on the record['Harvest']['UniqueIdentifier']
        # and return them as a list for use in the deactivation process
        unique_filters = [record['Harvest']['UniqueIdentifier'] for record in data]

        return unique_filters

    def deactivate_records(self, unique_filters: List[str]) -> dict:
        """
        This method deactivates records that were not found in the current collection based on their unique filters.

        Args:
            unique_filters (List[str]): The list of unique filters for the records to deactivate.

        Returns:
            dict: The result of the deactivation operation.
        """
        try:
            from datetime import datetime, timezone
            from ..silos import get_silo

            # Deactivate Records that were not found in this data collection operation (assumed to be inactive)
            # We filter on the following fields to ensure we don't deactivate records that are collected in other processes:
            # - UniqueIdentifier not in the list of unique filters
            # - Account
            # - Region
            deactivate_records_start = datetime.now(tz=timezone.utc)
            deactivation_timestamp = datetime.now(tz=timezone.utc).isoformat()

            silo = get_silo(self.task_chain.destination_silo)

            deactivated_replacements = silo.connect()[silo.database][self.task_chain.replacement_collection_name].update_many(
                filter={
                    'Harvest.UniqueIdentifier': {'$nin': unique_filters},
                    'Harvest.Account': self.task_chain.account,
                    'Harvest.Region': self.task_chain.region
                },
                update={
                    '$set': {
                        'Harvest.Active': False,
                        'Harvest.DeactivatedOn': deactivation_timestamp
                    }
                }
            )

            # Record the deactivation operation in the Task metadata
            self.meta['Stages'].append({'DeactivateDocuments': {
                'StartTime': deactivate_records_start,
                'DeactivatedDocuments': {
                    'matched': deactivated_replacements.matched_count,
                    'modified': deactivated_replacements.modified_count
                },
                'EndTime': datetime.now(tz=timezone.utc)
            }})

            # Deactivate Metadata records that were not found in this data collection operation (assumed to be inactive)
            # Deactivate Records that were not found in this data collection operation (assumed to be inactive)
            # We filter on the following fields to ensure we don't deactivate records that are collected in other processes:
            # - UniqueIdentifier not in the list of unique filters
            # - Silo
            # - Collection
            # - Account
            # - Region
            deactivate_metadata_start = datetime.now(tz=timezone.utc)
            silo = get_silo('harvest-core')
            deactivated_metadata = silo.connect()[silo.database]['metadata'].update_many(

                filter={
                    'UniqueIdentifier': {'$nin': unique_filters},
                    'Silo': self.task_chain.destination_silo,
                    'Collection': self.task_chain.replacement_collection_name,
                    'Harvest.Account': self.task_chain.account,
                    'Harvest.Region': self.task_chain.region
                },
                update={
                    '$set': {
                        'Active': False,
                        'DeactivatedOn': deactivation_timestamp
                    }
                }
            )

            # Record the deactivation operation in the Task metadata
            self.meta['Stages'].append({'DeactivateMetadata': {
                'StartTime': deactivate_metadata_start,
                'DeactivatedMetadata': {
                    'matched': deactivated_metadata.matched_count,
                    'modified': deactivated_metadata.modified_count
                },
                'EndTime': datetime.now(tz=timezone.utc)
            }})

        except Exception as ex:
            from traceback import format_exc
            ex_details = format_exc()
            raise TaskException(self, f'Error deactivating records. {str(ex)}')

        else:
            return {
                'Replacements': {
                    'matched': deactivated_replacements.matched_count,
                    'modified': deactivated_replacements.modified_count
                },
                'Metadata': {
                    'matched': deactivated_metadata.matched_count,
                    'modified': deactivated_metadata.modified_count
                }
            }


@register_definition(name='http', category='task')
class HttpTask(BaseTask):
    def __init__(self, url: str,
                 method: Literal['get', 'post', 'put', 'delete'] = 'get',
                 auth: dict = None,
                 cert: str = None,
                 data: dict = None,
                 content_type: str = 'application/json',
                 headers: dict = None,
                 verify: bool = True,
                 *args, **kwargs):

        """
        Initializes a new instance of the HttpTask class. This class is used to perform HTTP requests.
        """

        super().__init__(*args, **kwargs)

        self.url = url
        self.http_method = method.upper()       # cannot be named "method" because it conflicts with the method() function
        self.auth = auth
        self.cert = cert
        self.content_type = content_type
        self.data = data or {}
        self.headers = headers or {}
        self.verify = verify

        self.headers['User-Agent'] = f'CloudHarvest'


    def method(self, *args, **kwargs) -> 'BaseTask':
        """
        Executes the task.

        Returns:
            HttpTask: The current instance of the HttpTask class.
        """

        from requests import request
        from json import dumps

        # Perform the HTTP request
        response = request(
            method=self.http_method,
            url=self.url,
            auth=self.auth,
            data=dumps(self.data),
            headers=self.headers,
            cert=self.cert,
            verify=self.verify,
        )

        # Store the response in the result attribute
        self.result = response.json()

        return self

@register_definition(name='json', category='task')
class JsonTask(BaseTask):
    def __init__(self, mode: Literal['serialize', 'deserialize'], data: Any, default_type: type = str,
                 parse_datetimes: bool = False, *args, **kwargs):
        """
        Initializes a new instance of the JsonTask class.

        Args:
            mode (Literal['serialize', 'deserialize']): The mode in which to operate. 'load' reads a JSON file, 'dump' writes a JSON file.
            data (Any): The data to load or dump. Defaults to None.
            default_type (type, optional): The default type to use when loading JSON data. Defaults to str.
            parse_datetimes (bool, optional): A boolean indicating whether to parse datetimes in the JSON data.
                Attempts to parse a string as a datetime object. If the string cannot be parsed, it is returned as-is.
                Defaults to False.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.data = data
        self.mode = mode
        self.default_type = default_type
        self.parse_datetimes = parse_datetimes

    def method(self):
        """
        Executes the task.

        Returns:
            JsonTask: The current instance of the JsonTask class.
        """
        def do_mode(data: Any):
            import json
            if self.mode == 'deserialize':
                # Make sure the data is a string, otherwise it has already been deserialized
                if isinstance(data, str):
                    deserialized = json.loads(data)

                else:
                    deserialized = data

                if self.parse_datetimes:
                    def parse_datetime(v: Any):
                        """
                        Attempts to parse a string as a datetime object. If the string cannot be parsed, it is returned as-is.
                        """
                        from datetime import datetime

                        try:
                            return datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')

                        except Exception as ex:
                            return v

                    if isinstance(deserialized, dict):
                        for key, value in deserialized.items():
                            deserialized[key] = parse_datetime(value)

                    elif isinstance(deserialized, list):
                        for i, item in enumerate(deserialized):
                            deserialized[i] = parse_datetime(item)

                    else:
                        deserialized = parse_datetime(deserialized)

                return deserialized


            # Convert the data into a string
            elif self.mode == 'serialize':
                # default=str is used to serialization values such as datetime objects
                # This can lead to inconsistencies in the output, but it is necessary

                return json.dumps(data, default=str)


        # Check if self.data is an iterable
        if isinstance(self.data, (list, tuple)):
            self.result = [do_mode(d) for d in self.data]

        else:
            self.result = do_mode(self.data)

        return self


@register_definition(name='mongo', category='task')
class MongoTask(BaseDataTask):
    """
    The MongoTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a MongoDB database.
    """

    # The user filter class and stage are used to apply user filters to the database query results.
    FILTER_STAGE = 'start'

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

        self.collection = collection
        self.result_attribute = result_attribute

        from filters import MongoFilter
        self.filters = MongoFilter(self.filters)

    def apply_filters(self) -> 'MongoTask':
        """
        Applies user filters to the database configuration.
        """

        if self.filters.accepted is None or self.ignore_filters:
            return self

        with MongoFilter() as mongo_filter:
            mongo_filter.pipeline = self.arguments.get('pipeline')
            mongo_filter.apply()

        return self

    def method(self, *args, **kwargs):
        """
        Runs the task. This method will execute the method defined in `self.command` on the database or collection and
        store the result in the result attribute. `self.result_attribute` is used to extract the desired attribute
        from the result, if applicable.
        """

        # If connected, return existing connection otherwise connect
        # from pymongo import MongoClient
        # silo_config = self.silo.__dict__()
        # silo_config.pop('engine')
        # silo_config.pop('database')
        #
        # silo_extend = silo_config.pop('extended_db_configuration', {})
        # connection_config = silo_config | silo_extend
        # client = MongoClient(**connection_config)
        # si = client.server_info()

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

        if isinstance(result, (GeneratorType, CursorType, Cursor)):
            result = list(result)

        # Record the result
        self.result = result

        return self

@register_definition(name='redis', category='task')
class RedisTask(BaseDataTask):
    """
    The RedisTask class is a subclass of the BaseDataTask class. It represents a task that interacts with a
    Redis database.

    The Redis Python's connection class is not directly accessible via this DataTask. Instead, we provide some common
    database operations as methods named 'redis_' followed by the function, such as 'redis_get'.

    >>> task = RedisTask(
    >>>     command='get',
    >>>     arguments={'key': 'my_key'}
    >>> )
    """

    from redis import StrictRedis

    # These are the data types permitted in Redis. We use this list to evaluate if a value must be serialized before
    # being written to the Redis database.
    VALID_REDIS_TYPES = (str or int or float)

    def __init__(self, expire: int = None, serialization: bool = False, *args, **kwargs):
        """
        Initializes a new instance of the RedisTask class.

        Args:
        expire (int, optional): The expiration time for records in the Redis database. Defaults to None.
        serialization (bool, optional): When True: data being written will be serialized while data read will be deserialized. Defaults to False.

        """

        # Initialize the BaseDataTask class
        super().__init__(*args, **kwargs)

        self.expire = expire
        self.serialization = serialization

        # Validate the RedisTask configuration
        if not hasattr(self, f'redis_{self.command}'):
            methods = [
                method[6:] for method in dir(self)
                if method.startswith('redis_')
            ]

            raise TaskException(self, f"Invalid command '{self.command}' for RedisTask. Must be one of {methods}.")

    def method(self) -> 'RedisTask':
        """
        Executes the 'self.command' on the Redis database. Each offered StrictRedis command is prefixed with 'redis_'.
        See the individual methods for more information.

        It was necessary to break each offered command into different methods due to the complexity
        of the Redis api and the need to serialize and deserialize data.
        """

        result = self.walk_result_command_path(
            getattr(self, f'redis_{self.base_command_part}')()
        )

        self.result = result

        return self

    def redis_delete(self) -> dict:
        """
        Deletes the records from the Redis database based on a list of 'keys' or a 'pattern'.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.delete

        Arguments
        keys (List[str], optional): A list of keys to delete. Defaults to None.
        pattern (str, optional): A pattern to match keys. Defaults to None.

        Example:
        >>> # Delete records with keys 'key1' and 'key2'
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'delete',
        >>>         'arguments': {
        >>>             'keys': ['key1', 'key2']
        >>>             }
        >>>     }
        >>> }
        >>>
        >>> # Delete keys based on a pattern
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'delete',
        >>>         'arguments': {
        >>>             'pattern': 'key*'
        >>>             }
        >>>     }
        >>> }
        >>>
        >>> # Delete all keys
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'delete',
        >>>     }
        >>> }

        """

        # Retrieve keys based on the pattern or keys provided
        keys = self.redis_keys()

        delete_count = self.silo.connect().delete(*keys)

        result = {
            'deleted': delete_count,
            'keys': keys
        }

        return result

    def redis_expire(self) -> list:
        """
        Sets the expiration time for records in the Redis database based on a list of keys or a pattern.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.expire

        Arguments
        expire (int): The expiration time in seconds.
        keys (List[str], optional): A list of keys to expire. Defaults to None.
        pattern (str, optional): A pattern to match keys. Defaults to None.

        Example:
        >>> # Delete records with keys 'key1' and 'key2'
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'expire',
        >>>         'arguments': {
        >>>             'expire': 3600,         # (seconds) Expire in 1 hour
        >>>             'keys': ['key1', 'key2']
        >>>             }
        >>>     }
        >>> }
        """

        keys = self.redis_keys()

        for key in keys:
            self.calls += 1
            self.silo.connect().expire(name=key, time=self.arguments['expire'])

        self.result = {'keys': keys}

        return keys

    def redis_flushall(self):
        """
        Removes all records from the Redis database. This action is not recommended and may result in data loss.
        Instead, make sure to expire records when they are no longer needed.

        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.flushall

        Example:
        >>> # Delete all records from the database
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-write',
        >>>         'command': 'flushall',
        >>>     }
        >>> }

        """

        self.calls += 1

        delete_count = self.silo.connect().flushall()

        result = {
            'deleted': delete_count
        }

        return result

    def redis_get(self) -> list:
        """
        Gets the records from the Redis database based on a list of keys or a patterns. Returns a list of records. The
        return for this function is always a list of dictionaries. '_name' is included in the dictionary to indicate the
        name of the record.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.get

        Arguments
        names (str or List[str], optional): One or a list of names to retrieve. Defaults to None.
        keys (str or List[str], optional): One or a list of keys to retrieve. Defaults to None. Requires 'names' or 'patterns'.
        patterns (str or List[str], optional): One or a list of patterns to match keys. Defaults to None.

        Example:
        >>> # Retrieve records named 'key1' and 'key2'
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'get',
        >>>         'arguments': {
        >>>             'names': ['key1', 'key2']
        >>>             }
        >>>     }
        >>> }

        >>> # Retrieve records based on a patterns
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'get',
        >>>         'arguments': {
        >>>             'patterns': ['key*']
        >>>             }
        >>>     }
        >>> }
        """

        results = []

        def _get(n: str) -> dict:
            """
            Performs a simple get() operation on the Redis database.

            Arguments
            n (str): The name of the key to retrieve.

            Returns
            dict: The record retrieved from the Redis database. When the record is a simple value, it is stored in the 'value' field.
            """

            try:
                self.calls += 1

                r = self.silo.connect().get(name=n)

                if self.serialization:
                    from json import loads
                    r = loads(r)

            except Exception as ex:
                self.meta['Errors'].append(f"Error retrieving key '{n}': {str(ex)}")
                raise TaskException(self, f"Error retrieving key '{n}': {str(ex)}")

            else:
                if isinstance(r, dict):
                    r['_id'] = n

                else:
                    r = {
                        '_id': n,
                        'value': r
                    }

                return r

        # List of names
        names = self.arguments.get('names')
        if isinstance(names, str):
            names = [names]

        # List of keys
        keys = self.arguments.get('keys')
        if keys and isinstance(keys, str):
            keys = [keys]

        # Patterns to match keys
        patterns = self.arguments.get('patterns')
        if patterns and isinstance(patterns, str):
            patterns = [patterns]

        # HGET operations combine NAMES/PATTERN with a list of KEYS
        if (names and keys) or (patterns and keys):
            names = self.redis_keys() if patterns else names

            for name in names:
                if keys == ['*']:
                    # HGETALL operation returns all keys for the given name
                    self.calls += 1
                    result = self.silo.connect().hgetall(name=name)

                else:
                    # HGET operation
                    result = {}
                    for key in keys:
                        self.calls += 1
                        result[key] = self.silo.connect().hget(name=name, key=key)

                # Deserialize the result if necessary
                if self.serialization:
                    from json import loads, JSONDecodeError

                    for key, value in result.items():
                        try:
                            result[key] = loads(value)

                        except JSONDecodeError:
                            result[key] = value

                # Add the name field to the record
                result['_id'] = name

                # Append this name result to the results list
                results.append(result)

        # GET operations
        elif patterns:
            # Retrieve the record names based on the pattern
            names = self.redis_keys()

            # Get the records
            [
                results.append(_get(n=name))
                for name in names
            ]

        elif names:
            # Get operation
            [
                results.append(_get(n=name))
                for name in names
            ]

        else:
            raise TaskException(self, f'Invalid arguments. Correct argument combinations are: (names, keys), (names, patterns), (names), (patterns), (keys).')

        return results

    def redis_keys(self) -> list:
        """
        Gets the keys from the Redis database based on a pattern. If 'keys' are provided, the keys are returned as-is.
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.keys

        Example
        >>> # Returns a list of keys based on a pattern
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'keys',
        >>>         'arguments': {
        >>>             'pattern': 'key*'
        >>>             }
        >>>     }
        >>> }
        >>>
        >>> # Returns a list of all keys in the database
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'my-redis-read',
        >>>         'command': 'keys'
        >>>     }
        >>> }
        """

        # Use pattern matching to return a list of keys
        if self.arguments.get('patterns'):
            patterns = self.arguments.get('patterns')

            if isinstance(patterns, str):
                patterns = [patterns]

            result = []
            for pattern in patterns:
                result += self.silo.connect().scan_iter(match=pattern)

        # If the keys are provided, return the keys
        elif self.arguments.get('keys'):
            result = self.arguments.get('keys')

        # Return all keys
        else:
            result = self.silo.connect().scan_iter(match='*')

        return result

    def redis_set(self):
        """
        Writes records to the Redis database. If the record already exists, it will be overwriten.

        This method operates in two modes: SET and HSET. The mode is determined by the keys provided by the Task
        Configuration.

        Providing 'name' and 'value' implements 'SET'.
        Providing 'name' and 'keys' uses 'HSET'.

        Note that 'item.key_name' returns the value of that key, not the name of the key. If a static name should be
        provided, use a string which does not begin with 'var.' or 'item.' which reference the TaskChain's variables.

        SET
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.set
        This is used when the data consists of a simple name/value pair. In the following example, we use a simple
        dictionary of 'name' and 'value' which are then stored in the database. 'var_my_record' represents a variable
        stored in TaskChain.variables.
        >>> # Represents a variable 'my_record' stored in 'TaskChain.variables'.
        >>> var_my_record = {'name': 'myname', 'value': 'myvalue'}
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'arguments': {
        >>>             'name': 'var.my_record.name',                # 'myname'
        >>>             'value': 'var.my_record.value'               # 'myvalue'
        >>>         }
        >>>     }
        >>> }

        Use the 'iteration' directive when there is a list of records which needs to be stored. In this example, two new
        RedisTasks will be created, one for each record.
        >>> # Represents a variable 'my_records' stored in 'TaskChain.variables'.
        >>> var_my_records = [{'name': 'Bob', 'age': 28}, {'name': 'Susan', 'age': 30}]
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'arguments': {
        >>>             'name': 'item.name',        # Bob, Susan
        >>>             'value': 'item.age'         # 28, 30
        >>>         },
        >>>         'iteration': 'var.my_records'
        >>>     }
        >>> }

        HSET
        https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.cluster.RedisClusterCommands.hset
        This command is used when the data consists of 'name' and 'keys', where 'keys' is a list of keynames matching
        the desired keys to be stored in the database. The special value of '*' can be given for all keys.

        >>> # Represents a variable 'my_record' stored in 'TaskChain.variables'.
        >>> var_my_record = {'name': 'myname', 'value': 'myvalue'}
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'data': 'var.my_record'                 # Data must be supplied here
        >>>         'arguments': {
        >>>             'name': 'var.my_record.name',       # 'myname'
        >>>             'keys': ['value']                   # Alternatively: '*' or 'var.my_record.keys()'; includes 'name' field
        >>>         }
        >>>     }
        >>> }

        Use the 'iteration' directive to record many different records using HSET.
        >>> # Represents a variable 'my_records' stored in 'TaskChain.variables'.
        >>> var_my_records = [{'name': 'Bob', 'age': 28, 'eye': 'brown'}, {'name': 'Susan', 'age': 30, 'eye': 'green'}]
        >>>
        >>> # Example of the task configuration
        >>> task = {
        >>>     'redis': {
        >>>         'silo': 'redis-write',
        >>>         'command': 'set',
        >>>         'data': 'var.my_records',       # Data must be supplied here
        >>>         'arguments': {
        >>>             'name': 'item.name',        # [Bob], [Susan]
        >>>             'keys': ['age', 'eye']      # [28, brown], [30, green]
        >>>         },
        >>>         'iteration': 'var.my_records'
        >>>     }
        >>> }
        """

        results = {
            'added': 0,
            'errors': 0,
            'updated': 0
        }

        name = self.arguments.get('name')
        keys = self.arguments.get('keys')
        value = self.arguments.get('value')

        def record_response_code(r: int):
            """
            Records the response code from the Redis operation.
            """
            if r == -1:
                results['errors'] += 1

            elif r == 0:
                results['updated'] += 1

            else:
                results['added'] += 1

        # SET operation
        try:
            if name and value:
                self.calls += 1
                record_response_code(self.silo.connect().set(name=name, value=self.serialize(value), ex=self.expire))

            # HSET operation
            elif name and keys:
                if not self.data:
                    raise TaskException(self, "When 'name' and 'keys' are supplied, the 'data' attribute must be provided.",
                                        "This allows the task to iterate over the keys within the data and store them in "
                                        "the database.")

                # If keys is '*', we treat this as a wildcard operation and iterate over all keys in the data
                if isinstance(keys, str) and keys == '*':
                    keys = list(self.data.keys())

                record_response_code(self.silo.connect().hset(name=name,
                                                          mapping={
                                                              key: self.serialize(self.data[key])
                                                              for key in keys
                                                          })
                                     )

                # If an expiration time is provided, set the expiration time for this record
                if self.expire:
                    self.calls += 1
                    record_response_code(self.silo.connect().expire(name=name, time=self.expire))

            else:
                raise TaskException(self, "Invalid argument combination. Must provide ('name', 'value') or ('name', 'keys').")

        except Exception as ex:
            self.meta['Errors'].append(str(ex))

        return results

    def deserialize(self, v: Any) -> Any:
        """
        Deserializes the value if indicated by the task configuration.

        Arguments:
        v (Any): The value to deserialize.
        """

        if self.serialization and isinstance(v, str):
            from json import loads
            return loads(v)

        else:
            return v

    def serialize(self, v: Any) -> Any:
        """
        Serializes the value if indicated by the task configuration.

        Arguments:
        v (Any): The value to serialize.
        """

        if self.serialization and not isinstance(v, self.VALID_REDIS_TYPES):
            from json import dumps
            return dumps(v, default=str)

        else:
            return v

@register_definition(name='wait', category='task')
class WaitTask(BaseTask):
    def __init__(self,
                 check_time_seconds: float = 1,
                 when_after_seconds: float = 0,
                 when_all_previous_async_tasks_complete: bool = False,
                 when_all_previous_tasks_complete: bool = False,
                 when_all_tasks_by_name_complete: List[str] = None,
                 when_any_tasks_by_name_complete: List[str] = None,
                 **kwargs):

        """
        The WaitTask class is a subclass of the BaseTask class. It represents a task that waits for certain conditions
        to be met before proceeding. It is most useful in task chains where asynchronous tasks are used.

        Initializes a new instance of the WaitTask class.

        Args:
            check_time_seconds (float, optional): The time interval in seconds at which this task checks if its conditions are met. Defaults to 1.
            when_after_seconds (float, optional): The time in seconds that this task should wait before proceeding. Defaults to 0.
            when_all_previous_async_tasks_complete (bool, optional): A flag indicating whether this task should wait for all previous async tasks to complete. Defaults to False.
            when_all_previous_tasks_complete (bool, optional): A flag indicating whether this task should wait for all previous tasks to complete. Defaults to False.
            when_all_tasks_by_name_complete (List[str], optional): A list of task names. This task will wait until all tasks with these names are complete. Defaults to None.
            when_any_tasks_by_name_complete (List[str], optional): A list of task names. This task will wait until any task with these names is complete. Defaults to None.
        """

        self.check_time_seconds = check_time_seconds
        self._when_after_seconds = when_after_seconds
        self._when_all_previous_async_tasks_complete = when_all_previous_async_tasks_complete
        self._when_all_previous_tasks_complete = when_all_previous_tasks_complete
        self._when_all_tasks_by_name_complete = when_all_tasks_by_name_complete
        self._when_any_tasks_by_name_complete = when_any_tasks_by_name_complete

        super().__init__(**kwargs)

    def method(self, *args, **kwargs):
        """
        Runs the task. This method will block until the conditions specified by the task attributes are met.
        """
        from time import sleep

        while True:
            if any([
                self.when_after_seconds,
                self.when_all_previous_async_tasks_complete,
                self.when_all_previous_tasks_complete,
                self.when_all_tasks_by_name_complete,
                self.when_any_tasks_by_name_complete,
                self.status == TaskStatusCodes.terminating
            ]):
                break

            sleep(self.check_time_seconds)

    @property
    def when_after_seconds(self) -> bool:
        """
        Checks if the allotted seconds have passed since this Task started. This method requires that super.on_start()
        is run at, at the least, that `self.start` is populated with a UTC datetime object.
        """

        from datetime import datetime, timezone

        if self._when_after_seconds > 0:
            if isinstance(self.start, datetime):
                return  (datetime.now(tz=timezone.utc) - self.start).total_seconds() > self._when_after_seconds

    @property
    def when_all_previous_async_tasks_complete(self) -> bool:
        """
        Checks if all previous async tasks are complete.

        Returns:
            bool: True if all previous AsyncTasks are complete, False otherwise.
        """

        if self._when_all_previous_async_tasks_complete:
            return all([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.task_chain[0:self.position]
                if task.blocking is False
            ])

    @property
    def when_all_previous_tasks_complete(self) -> bool:
        """
        Checks if all previous tasks are complete.

        Returns:
            bool: True if all previous tasks are complete, False otherwise.
        """

        if self._when_all_previous_tasks_complete:
            return all([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.task_chain[0:self.position]
            ])

    @property
    def when_all_tasks_by_name_complete(self) -> bool:
        """
        Checks if all tasks with the specified names are complete.

        Returns:
            bool: True if all tasks with the specified names are complete, False otherwise.
        """

        if self._when_all_tasks_by_name_complete:
            return all([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.task_chain[0:self.position]
                if task.name in self._when_all_tasks_by_name_complete
            ])

    @property
    def when_any_tasks_by_name_complete(self) -> bool:
        """
        Checks if any task with the specified names is complete.

        Returns:
            bool: True if any task with the specified names is complete, False otherwise.
        """

        if self._when_any_tasks_by_name_complete:
            return any([
                task.status in [
                    TaskStatusCodes.complete, TaskStatusCodes.error
                ]
                for task in self.task_chain[0:self.position]
                if task.name in self._when_all_tasks_by_name_complete
            ])

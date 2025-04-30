from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.dataset import WalkableDict
from CloudHarvestCoreTasks.tasks.base import BaseTask
from CloudHarvestCoreTasks.exceptions import TaskException

from typing import List
from pymongo import ReplaceOne


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
        'UniqueIdentifierKeys',         # UniqueIdentifierKeys requires at least one value, so .0 is expected
        'Module.Author',                # The author of the Harvest module
        'Module.Name',                  # The name of the Harvest module that collected the data
        'Module.Url',                   # The repository where the Harvest module is stored
        'Module.Version',               # The version of the Harvest module
        'Dates.LastSeen',               # The date indicating when the record was last collected by Harvest
        'Active'                        # A boolean indicating if the record is active
    )

    def __init__(self, *args, **kwargs):
        """
        Initializes a new instance of the HarvestRecordUpdateTask class. This class is used to update records in the
        destination silo, pstar, and the metadata silos with the data collected in the BaseHarvestTaskChain. This task
        is automatically added to the end of the task chain by the BaseHarvestTaskChain class.

        HarvestRecordUpdateTask accepts no arguments. They are instead supplied by the BaseHarvestTaskChain. Indeed,
        the HarvestRecordUpdateTask exists to separate the functionality of updating records from the BaseHarvestTaskChain
        so that metrics for the update process can be collected separately from the data collection.

        Although not an argument, the HarvestRecordUpdateTask requires that it have a parent task chain that is an
        instance of BaseHarvestTaskChain. This is enforced in the __init__ method.
        """

        super().__init__(*args, **kwargs)

        # Ensure that the task chain is a BaseHarvestTaskChain
        from CloudHarvestCoreTasks.chains.harvest import BaseHarvestTaskChain
        if not isinstance(self.task_chain, BaseHarvestTaskChain):
            raise TaskException(self, 'HarvestTask must be used in a BaseHarvestTaskChain.')

        # Type hint for the task_chain attribute
        from typing import cast
        self.task_chain = cast(BaseHarvestTaskChain, self.task_chain)

    @property
    def pstar_identifier(self) -> dict:
        return {
            'Platform': self.task_chain.platform,
            'Service': self.task_chain.service,
            'Type': self.task_chain.type,
            'Account': self.task_chain.account,
            'Region': self.task_chain.region or 'global'
        }

    def method(self) -> 'HarvestUpdateTask':
        """
        Executes the task.

        Returns
            HarvestUpdateTask: The current instance of the HarvestTask class.
        """

        self.meta['Stages'] = []

        # Validate the Task can reach the required silos
        from CloudHarvestCoreTasks.silos import get_silo
        for silo_name in (self.task_chain.destination_silo, 'harvest-core'):
            try:
                get_silo(silo_name).connect().server_info()

            except Exception as ex:
                raise TaskException(self, f'Unable to connect to the {silo_name} silo. {str(ex)}')

        # Attach metadata to the records
        data = self.attach_metadata_to_records()

        # Bulk Replace the records in the destination silo and the metadata in the metadata silo
        unique_identifiers = self.replace_bulk_records(data)

        # Deactivate records that were not found in this data collection operation on the destination silo and the metadata silo
        deactivation_results = self.deactivate_records(unique_identifiers)

        self.result = {
            'RecordsProcessed': len(data),
            'RecordsReplaced': len(unique_identifiers),
            'DeactivationResults': deactivation_results
        }

        self.record_pstar()

        return self

    def attach_metadata_to_records(self) -> List[dict]:
        """
        This method attaches metadata to the records in the data list. It also generates the UniqueIdentifier for each record.

        Returns
            List[dict]: The list of records with the attached metadata.
        """
        from copy import deepcopy

        metadata = self.build_metadata()

        # Take the result from the second to last task in the task chain and attach the metadata to each record
        # The last task in the chain is this harvest_update task, so results must be from the preceding task
        data = self.task_chain.variables.get('result') or self.task_chain[-2].result or []

        # Make sure the data is a DataSet object
        from CloudHarvestCoreTasks.dataset import DataSet
        data = DataSet(data) if not isinstance(data, DataSet) else data

        for record in self.task_chain.variables.get('result') or self.task_chain[-2].result or []:
            # Make sure the Harvest metadata field exists
            if 'Harvest' not in record.keys():
                record['Harvest'] = {}

            # Merges any existing metadata with the new metadata, overwriting any existing fields
            record['Harvest'] |= deepcopy(metadata)

            # Generate this record's unique filter, which is a single string which combines the values of the  unique
            # identifier keys. We accept that some values might be null and ignore them.
            unique_identifier = []
            for field in metadata['UniqueIdentifierKeys']:
                field_value = record.walk(field)

                if field_value is not None:
                    unique_identifier.append(str(field_value))

            unique_identifier = '-'.join(unique_identifier)

            # Attach existing metadata to the record
            record['Harvest']['UniqueIdentifier'] = unique_identifier

            # The 'Account' field is a special case. It can be either the AccountName or the AccountId, depending on the data source.
            record['Harvest']['Account'] = record.walk('Harvest.AccountName') or record.walk('Harvest.AccountId') or record.walk('Harvest.Account')

        return data

    def build_metadata(self) -> dict:
        """
        This method generates metadata for the task chain based on the class attributes and the task chain's metadata.
        """
        from datetime import datetime, timezone

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
        result = WalkableDict(pstar | build_components | dates | silo)

        # Validate that all required metadata fields are present
        missing_fields = [
            field for field in self.REQUIRED_METADATA_FIELDS
            if result.walk(field) is None
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

        Returns
            list: The list of unique filters for the records that were processed.
        """
        replacements = []
        metadata =[]

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
            extras = {
                field: WalkableDict(record).walk(field)
                for field in self.task_chain.extra_metadata_fields
            }

            replace_meta = ReplaceOne(filter=replace_filter,
                                      replacement=record['Harvest'] | {'Tags': record.get('Tags') or {}} | extras,
                                      upsert=True)

            replacements.append(replace_resource)
            metadata.append(replace_meta)

        # Perform database replacement operations
        if replacements:
            # Replace the existing resource records
            replacement_results = self.bulk_replace(silo_name=self.task_chain.destination_silo,
                                                    collection=self.task_chain.replacement_collection_name,
                                                    prepared_replacements=replacements)

            self.meta['Stages'].append({'BulkReplaceDocuments': replacement_results})


        if metadata:
            # Replace the existing metadata records
            metadata_results = self.bulk_replace(silo_name='harvest-core',
                                                 collection='metadata',
                                                 prepared_replacements=metadata)

            # Store the results in the metadata
            self.meta['Stages'].append({'BulkReplaceMetadata': metadata_results})

        # Gather ObjectId's of all the records that were processed based on the record['Harvest']['UniqueIdentifier']
        # and return them as a list for use in the deactivation process
        return [
            record['Harvest']['UniqueIdentifier']
            for record in data
        ]

    def deactivate_records(self, unique_identifiers: List[str]) -> dict:
        """
        This method deactivates records that were not found in the current collection based on their unique filters.

        Args:
            unique_identifiers (List[str]): The list of unique filters for the records to deactivate.

        Returns
            dict: The result of the deactivation operation.
        """
        try:
            from datetime import datetime, timezone
            from CloudHarvestCoreTasks.silos import get_silo

            # Deactivate Records that were not found in this data collection operation (assumed to be inactive)
            # We filter on the following fields to ensure we don't deactivate records that are collected in other processes:
            # - UniqueIdentifier not in the list of unique filters
            # - Account
            # - Region

            deactivate_records_start = datetime.now(tz=timezone.utc)
            deactivation_timestamp = datetime.now(tz=timezone.utc).isoformat()

            from pymongo import MongoClient
            silo = get_silo(self.task_chain.destination_silo)
            client: MongoClient = silo.connect()

            collection = client[silo.database][self.task_chain.replacement_collection_name]

            if self.task_chain.mode == 'all':
                # Records to be deactivated
                deactivate_records = [
                    record['UniqueIdentifier']
                    for record in collection.find({
                        'Harvest.Account': self.task_chain.account,
                        'Harvest.Region': self.task_chain.region,
                    }, {'UniqueIdentifier': '$Harvest.UniqueIdentifier'})
                    if record not in unique_identifiers
                ]

            else:
                # More selective subset of entries
                deactivate_records = [
                    record
                    for record in self.task_chain.identifiers
                    if record not in unique_identifiers
                ]

            # Deactivate Records that were not found in this data collection operation (assumed to be inactive)
            deactivated_replacements = silo.connect()[silo.database][self.task_chain.replacement_collection_name].update_many(
                filter={
                    'Harvest.UniqueIdentifier': {'$in': deactivate_records},
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
                'EndTime': deactivate_records_start
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
                    'UniqueIdentifier': {'$nin': unique_identifiers},
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

    def record_pstar(self) -> 'HarvestUpdateTask':
        """
        This method records metadata about data collection operations in the harvest-core/pstar collection

        Returns
            HarvestUpdateTask: The current instance of the HarvestUpdateTask class.
        """

        from CloudHarvestCoreTasks.silos import get_silo
        from pymongo import MongoClient

        silo = get_silo('harvest-core')
        client: MongoClient = silo.connect()

        collection = client['harvest']['pstar']

        # Get the old PSTAR record
        old_pstar = collection.find_one(self.pstar_identifier) or {}

        original_count = old_pstar.get('Count') or 0

        from copy import deepcopy
        metrics = deepcopy(self.task_chain.result['metrics'][-1])     # Provides totals for all metric stages
        metrics['Stages'] = len(self.task_chain)

        if original_count > 0 and self.result['RecordsProcessed'] == 0:
            # Indicates that records were previously seen for this PSTAR, but no longer
            metrics['Records'] = -1

        else:
            metrics['Records'] = self.result['RecordsProcessed']

        result = self.pstar_identifier | metrics | {'Errors': self.task_chain.result['errors']}

        collection.replace_one(self.pstar_identifier, result, upsert=True)

        return self

    @staticmethod
    def bulk_replace(silo_name: str, collection: str, prepared_replacements: List[ReplaceOne]) -> dict:
        """
        This method performs a bulk Replace operation on the specified silo.

        Args:
            silo_name (str): The name of the silo where the records will be Replaced.
            collection (str): The name of the collection where the records will be Replaced.
            prepared_replacements (List[ReplaceOne]): The list of Replace operations to perform.

        Returns
            dict: The result of the Replace operation.
        """
        from datetime import datetime, timezone
        from CloudHarvestCoreTasks.silos import get_silo

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

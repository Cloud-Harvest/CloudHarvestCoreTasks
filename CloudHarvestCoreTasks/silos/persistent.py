"""
This module contains methods used to validate and record data in the persistent silo. The persistent silo is a MongoDB
database that stores data collected by Harvest. The data is stored in collections based on the Platform, Service, and
Type such as `aws.rds.instances` or `aws.ec2.instances`. Each record in the silo is uniquely identified by the
`Harvest.UniqueIdentifier` field, which is a combination of the `Module.FilterCriteria` values.
"""

from datetime import datetime, timezone
from logging import getLogger
from bson import ObjectId
from .silos import get_silo

logger = getLogger('harvest')

_CLIENTS = {}


# Persistent Silo data record operations and parameters
_flat_record_separator = '.'
_required_meta_fields = (
    'Platform',                    # The Platform (ie AWS, Azure, Google)
    'Service',                     # The Platform's service name (ie RDS, EC2, GCP)
    'Type',                        # The Service subtype, if applicable (ie RDS instance, EC2 event)
    'Account',                     # The Platform account name or identifier
    'Region',                      # The geographic region name for the Platform
    'Module.FilterCriteria.0',     # FilterCriteria requires at least one value, so .0 is expected
    'Module.Name',                 # The name of the Harvest module that collected the data
    'Module.Repository',           # The repository where the Harvest module is stored
    'Module.Version',              # The version of the Harvest module
    'Dates.DeactivatedOn',         # The date the record was deactivated, if applicable
    'Dates.LastSeen',              # The date indicating when the record was last collected by Harvest
    'Active'                       # A boolean indicating if the record is active
)


def set_pstar(silo_name: str, **kwargs) -> ObjectId:
    """
    A PSTAR is a concept in Harvest where objects are stored on five dimensions: Platform, Service, Type, Account, and
    Region. This function writes a PSTAR record to the persistent silo.

    Parameters
    silo_name (str): the name of the silo to write the record to
    Platform (str): the cloud provider this database was retrieved from (ie AWS, Azure, Google)
    Service (str): the provider's service (ie "RDS", "EC2")
    Type (str): service's object classification (ie RDS "instance" or EC2 "event")
    Account (str): a unique identifier indicating the account or environment level for this service
    Region (str): the geographic region name for the objects retrieved from the underlying API call
    Count (int): number of records retrieved in the data collection job
    StartTime (datetime): when the data collection job was started
    EndTime (datetime): when the data collection job completed
    ApiVersion (str): version of this software
    Module (dict): metadata of the collector used to collect the data
    Errors (list): provides any error messages

    Returns
    ObjectId: ObjectId of the inserted/updated record

    Example
    >>> kwargs = {
    >>>     'Platform': 'AWS',
    >>>     'Service': 'RDS',
    >>>     'Type': 'instance',
    >>>     'Account': '123456789012',
    >>>     'Region': 'us-west-2',
    >>>     'Count': 10,
    >>>     'StartTime': datetime(2023, 10, 1, 12, 0, 0),
    >>>     'EndTime': datetime(2023, 10, 1, 12, 30, 0),
    >>>     'ApiVersion': '1.0.0',
    >>>     'Module': {
    >>>         'Name': 'harvest_module',
    >>>         'Version': '1.0.0',
    >>>         'Repository': 'https://github.com/example/repo',
    >>>         'FilterCriteria': ['criteria1', 'criteria2']
    >>>     },
    >>>     'Errors': []
    >>> }
    """

    client = get_silo(silo_name).connect()

    # no need to replicate this logic everywhere
    kwargs['duration'] = duration_in_seconds(a=kwargs['EndTime'], b=kwargs['StartTime'])

    _id = None
    try:
        from datetime import datetime
        _id = client['harvest']['pstar'].find_one_and_update(filter={k: kwargs.get(k) for k in ['Platform',
                                                                                                'Service',
                                                                                                'Type',
                                                                                                'Account',
                                                                                                'Region']},
                                                             projection={'_id': 1},
                                                             update={"$set": kwargs},
                                                             upsert=True).get('_id')

    except Exception as ex:
        logger.error(f'{client.log_prefix}: ' + ' '.join(ex.args))

    finally:
        return _id


def prepare_record(record: dict, meta_extra_fields: tuple = ()) -> tuple:
    """
    Prepares a record to be written to the persistent silo. To qualify as a valid record, it must contain a key named
    "Harvest" with the following structure:

    ```json
    {
        "Harvest": {
            "Platform": "str",
            "Service": "str",
            "Type": "str",
            "Account": "str",
            "Region": "str",
            "Module": {
                "Name": "str",
                "Version": "str",
                "Repository": "str",
                "FilterCriteria": ["list"]
            },
            "Dates": {
                "LastSeen": "datetime.datetime",
                "DeactivatedOn": "datetime.datetime"
            },
            "Active": "bool"
        }
    }
    ```

    Args:
        record (dict): A dictionary object representing a single record.
        meta_extra_fields (tuple): Extra fields to add to the meta collection.

    Returns:
        tuple: A tuple containing the collection name, the bulk write operation for the resource, and the bulk
        write operation for the meta record.
    """

    from pymongo import ReplaceOne

    # flatten the record so we can build the Harvest.UniqueIdentifier
    from flatten_json import flatten
    flat_record = flatten(record, separator=_flat_record_separator)
    unique_filter = get_unique_filter(record=record, flat_record=flat_record)

    if not unique_filter:
        raise ValueError('UniqueFilter not found in record', record)

    record['Harvest']['UniqueIdentifier'] = unique_filter

    # identify the target collection name from the metadata
    collection = get_collection_name(**record['Harvest'])

    # create the bulk write operation for this record
    replace_resource = ReplaceOne(filter=unique_filter,
                                  replacement=record,
                                  upsert=True)

    # create the bulk write operation for the meta record
    replace_meta = ReplaceOne(filter=unique_filter,
                              replacement={
                                  "Collection": collection,
                                  "UniqueIdentifier": unique_filter,
                                  "Harvest": record["Harvest"],
                                  **{k: record.get(k) or flat_record.get(k) for k in meta_extra_fields}
                              },
                              upsert=True)

    result = (collection, replace_resource, replace_meta)
    return result


def write_records(silo_name: str, records: list) -> list:
    """
    Writes a list of records to the persistent silo.

    Args:
        silo_name (str): The name of the silo to write the records to.
        records (list): A list of records to write to the cache.

    Returns:
        list: A list of results from the bulk write operation.
    """

    # Prepare the records for writing to the cache
    results = {
        'updated': [],
        'deactivated': [],
        'errors': []
    }

    bulk_records = {'meta': []}
    for record in records:
        write_record_result = prepare_record(record=record)

        # don't add this record to the bulk operation if it had an error during the write_record phase
        if isinstance(write_record_result, Exception):
            results['errors'].append((record, write_record_result))
            continue

        collection, record_replace, meta_replace = write_record_result

        if not bulk_records.get(collection):
            bulk_records[collection] = []

        bulk_records[collection].append(record_replace)
        bulk_records['meta'].append(meta_replace)

    # perform bulk writes by collection but always do 'meta' last
    client = get_silo(silo_name).connect()

    updated_records = [
        client['harvest'][collection].bulk_write(bulk_records[collection])
        for collection in list([k for k in bulk_records.keys()
                                if k not in 'meta'] + ['meta'])
    ]

    results['updated'] = updated_records

    return updated_records


def duration_in_seconds(a: datetime, b: datetime) -> int or float:
    """
    A simple function for retrieving the number of seconds between two dates.

    Args:
        a (datetime): The first datetime object.
        b (datetime): The second datetime object.

    Returns:
        An integer or float representing the number of seconds between two datetime objects
    """

    return abs((a - b).total_seconds())


def get_collection_name(**harvest_metadata) -> str:
    """
    Returns the collection name used in a PSTAR record write based on the Harvest metadata key.

    Args:
        harvest_metadata (dict): The Harvest metadata dictionary.

    Returns:
        str: The collection name.
    """

    return '.'.join([
        str(harvest_metadata['Platform']),
        str(harvest_metadata['Service']),
        str(harvest_metadata['Type'])
    ])


def get_unique_filter(record: dict, flat_record: dict) -> dict:
    """
    Retrieves the unique identifier defined by the module for this record.

    Args:
        record (dict): The record to extract the unique identifier from.
        flat_record (dict): The flattened version of the record.

    Returns:
        dict: The unique identifier for the record.
    """

    return {
        field: flat_record.get(field)
        for field in (record.get('Harvest', {}).get('Module', {}).get('FilterCriteria') or [])
    }

def deactivate_records(silo_name: str, collection_name: str, record_ids: list) -> dict:
    """
    Deactivate records in the cache by setting the Harvest.Active field to False and adding a DeactivatedOn date.

    Args:
        silo_name (str): The name of the silo in which to deactivate the records
        collection_name (str): The name of the collection in which to deactivate the records.
        record_ids (list): A list of record IDs to deactivate.
    """

    client = get_silo(silo_name).connect()

    collection = client['harvest'][collection_name]

    # deactivate records which were not inserted/updated in this write operation
    records_to_deactivate = [r["_id"] for r in collection.find({"Harvest.Active": True, "_id": {"$nin": record_ids}},
                                                               {"_id": 1})]

    update_set = {"$set": {"Harvest.Active": False,
                           "Harvest.Dates.DeactivatedOn": datetime.now(tz=timezone.utc)}}

    update_many = collection.update_many(filter={"_id": {"$in": records_to_deactivate}},
                                         update=update_set)

    # update the meta cache
    collection = client['harvest']['meta']
    update_meta = collection.update_many(filter={"Collection": collection_name,
                                                 "CollectionId": {"$in": records_to_deactivate}},
                                         update=update_set)

    logger.debug(f'{client.log_prefix}: harvest.{collection_name}: deactivated {update_many.modified_count}')

    return {
        'deactivated_ids': records_to_deactivate,
        'modified_count': update_many.modified_count,
        'meta_count': update_meta.modified_count
    }


def add_indexes(silo_name: str, indexes: dict):
    """
    Create an index in the backend cache.

    Args:
        silo_name (str): The name of the silo in which to create the indexes.
        indexes (dict): A dictionary containing the indexes to create.

    Returns:
        None
    """

    # Get the connection
    client = get_silo(silo_name).connect()

    # Identify databases
    for database in indexes.keys():

        # Identify collections
        for collection in indexes['harvest'].keys():

            # Identify indexes
            for index in indexes['harvest'][collection]:

                # Add single-field indexes defined as a list of strings
                if isinstance(index, (str, list)):
                    client['harvest'][collection].create_index(keys=index)
                    logger.debug(f'{client.log_prefix}: added index: {database}.{collection}.{str(index)}')

                # Add complex indexes defined as a dictionary
                elif isinstance(index, dict):

                    # pymongo is very picky and demands a list[tuple())
                    keys = [(i['field'], i.get('sort', 1)) for i in index.get('keys', [])]

                    client['harvest'][collection].create_index(keys=keys, **index['options'])

                    logger.debug(f'{client.log_prefix}: added index: {database}.{collection}.{str(index)}')

                else:
                    logger.error(f'unexpected type for index `{index}`: {str(type(index))}')


def check_harvest_metadata(flat_record: dict) -> bool:
    """
    Check if the record contains the required Harvest metadata fields.

    Parameters:
        flat_record (dict): The flattened record to check.
    """
    if not flat_record:
        return False

    for field in _required_meta_fields:
        field_name = _flat_record_separator.join(['Harvest', field])

        if field_name not in flat_record.keys():
            logger.warning('record failed harvest metadata check: ' + field_name)
            return False

    return True


def map_dicts(dict_list):
    """
    This function examines a list of dictionaries and generates an output of the keys and data types.

    Parameters:
    dict_list (list): A list of dictionaries to examine.

    Returns:
    dict: A dictionary representing the consolidated keys and their data types from the list of dictionaries.
    """

    # Initialize an empty dictionary to store the results
    result = {}

    def examine_data(data, prefix=''):
        """
        This function is a helper function that recursively traverses a data structure and collects the keys and their
        corresponding data types.

        Parameters:
        data: The data to examine. Can be any standard Python type.
        prefix (str): The prefix for the key (default is '').

        Returns:
        None
        """

        # If the data is a dictionary, iterate over its items
        if isinstance(data, dict):
            for k, v in data.items():
                examine_data(v, prefix + k + _flat_record_separator)

        # If the data is a list, iterate over its items
        elif isinstance(data, list):
            for i, item in enumerate(data):
                examine_data(item, prefix + f'{i}.')

        # If the data is a basic type (not a dict or list), add the key (with the prefix) and the type of the value to
        # the result dictionary
        else:
            key = prefix.rstrip('.')
            value_type = type(data).__name__

            # Only add the key-value pair if the key does not exist or the existing value is different
            if key not in result or result[key] != value_type:
                result[key] = value_type

    # Iterate over the dictionaries in the list and call the helper function with each dictionary
    [
        examine_data(d) for d in dict_list
    ]

    # Return the result dictionary
    from flatten_json import unflatten_list

    return unflatten_list(result, separator=_flat_record_separator)


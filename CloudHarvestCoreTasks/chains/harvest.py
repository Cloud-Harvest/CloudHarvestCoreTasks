from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.chains.base import BaseTaskChain

from typing import List, Literal


@register_definition(name='harvest', category='chain')
class BaseHarvestTaskChain(BaseTaskChain):
    """
    The BaseHarvestTaskChain class is a subclass of the BaseTaskChain class and is used to manage a sequence of tasks
    related to harvesting data. Specific functionality may be required based on the source data
    provider.
    """

    def __init__(self,
                 platform: str,
                 service: str,
                 type: str,
                 account: str,
                 region: str,
                 unique_identifier_keys: (str or List[str]),
                 destination_silo: str = 'harvest-core',
                 extra_matadata_fields: (str or List[str]) = None,
                 mode: Literal['all', 'single'] = 'all',
                 *args, **kwargs):

        """
        Initializes a new instance of the BaseHarvestTaskChain class.

        platform (str): The Platform (ie AWS, Azure, Google)
        service (str): The Platform's service name (ie RDS, EC2, GCP)
        type (str): The Service subtype, if applicable (ie RDS instance, EC2 event)
        account (str): The Platform account name or identifier
        region (str): The geographic region name for the Platform
        unique_identifier_keys (str or List[str]): The unique filter keys for the harvested data
        destination_silo (str, optional): The name of the destination silo where the harvested data will be stored
        extra_matadata_fields (str or List[str], optional): Additional metadata fields to include in the harvested data's metadata record
        mode (str, optional): The mode of the harvest task chain. 'all' will harvest all data, 'single' will harvest a single record

        Exposes
        The following parameters are exposed as variables in the task chain:
        - var.pstar: A dictionary containing the platform, service, type, account, and region.

        Configuration Example
        >>> {
        >>>   "name": "Example Harvest Task Chain",
        >>>   "description": "A task chain for harvesting data",
        >>>   "tasks": [
        >>>     {
        >>>       "task_name": "example_task",
        >>>       "result_as": "result",
        >>>       "task_parameters": {
        >>>         "param1": "value1",
        >>>         "param2": "value2"
        >>>       }
        >>>     }
        >>>   ],
        >>>   "max_workers": 4,
        >>>   "idle_refresh_rate": 3,
        >>>   "worker_refresh_rate": 0.5,
        >>>   "platform": "aws",                        # This should be populated by the
        >>>   "service": "ec2",
        >>>   "type": "instance",
        >>>   "account": "example_account",
        >>>   "region": "us-west-2",
        >>>   "destination_silo": "example_silo",
        >>>   "unique_identifier_keys": ["key1", "key2"],
        >>>   "extra_metadata_fields": ["field1", "field2"]
        >>> }
        """

        # Update the template based on the mode
        if isinstance(kwargs['tasks'], dict):
            if kwargs['tasks'].get('all') and kwargs['tasks'].get('single'):
                kwargs['tasks'] = kwargs['tasks']['mode']

        super().__init__(*args, **kwargs)

        # Set the class attributes
        self.platform = platform
        self.service = service
        self.type = type
        self.account = account
        self.region = region
        self.mode = mode
        self.destination_silo = destination_silo
        self.unique_identifier_keys = [unique_identifier_keys] if isinstance(unique_identifier_keys, str) else unique_identifier_keys
        self.extra_metadata_fields = [extra_matadata_fields] if isinstance(extra_matadata_fields, str) else extra_matadata_fields or []

        # Computed attributes
        self.replacement_collection_name = f'{self.platform}_{self.service}_{self.type}'

        # Insert a HarvestTask template into the end of the task chain
        template = {
            'harvest_update': {
                'name': f'{self.destination_silo}:{self.platform}/{self.service}/{self.type}/{self.account}/{self.region}',
                'description': 'Updates the Harvest Persistent Storage with the latest data',
                'data': 'var.result',
                'result_as': 'result',
                'platform': self.platform,
                'service': self.service,
                'type': self.type,
                'account': self.account,
                'region': self.region,
                'unique_identifier_keys': self.unique_identifier_keys,
            }
        }

        self.task_templates.append(template)

        # Expose the platform, service, type, account, and region as variables
        self.variables |= {
            'platform': self.platform,
            'service': self.service,
            'type': self.type,
            'account': self.account,
            'region': self.region,
        }

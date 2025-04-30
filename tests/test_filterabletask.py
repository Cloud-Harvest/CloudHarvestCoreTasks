from CloudHarvestCorePluginManager.registry import register_all
from CloudHarvestCoreTasks.dataset import DataSet
from CloudHarvestCoreTasks.factories import task_chain_from_dict
from CloudHarvestCoreTasks.tasks import TaskStatusCodes

import unittest

test_data = [
    {
        'name': 'John Doe',
        'address': {
            'street': '123 Main St',
            'city': 'Anytown',
            'state': 'CA',
            'zip': '12345'
        },
        'dob': '1990-01-01',
        'email': 'john.doe@example.com',
        'phone': '555-1234',
        'tags': ['friend', 'colleague'],
        'notes': 'Met at conference',
        'age': 30,
        'active': True
    },
    {
        'name': 'Jane Smith',
        'address': {
            'street': '456 Elm St',
            'city': 'Othertown',
            'state': 'TX',
            'zip': '67890'
        },
        'dob': '1985-05-15',
        'email': 'jane.smith@example.com',
        'phone': '555-5678',
        'tags': ['family'],
        'notes': 'Cousin',
        'age': 35,
        'active': False
    },
    {
        'name': 'Jane Smith',
        'address': {
            'street': '789 Oak St',
            'city': 'Newtown',
            'state': 'NY',
            'zip': '11223'
        },
        'dob': '1992-07-20',
        'email': 'jane.smith2@example.com',
        'phone': '555-9876',
        'tags': ['friend'],
        'notes': 'Met at school',
        'age': 28,
        'active': True
    }
]


class TestDataSetTaskFilters(unittest.TestCase):
    def setUp(self):
        register_all()

        self.dataset = DataSet()
        self.dataset.add_records(test_data.copy())

        self.task_chain_template = {
            'report': {
                'headers': ['name', 'address.state', 'dob'],
                'tasks': [
                    {
                        'dataset': {
                            'name': 'test_dataset',
                            'description': 'Filter the results from the dataset.',
                            'filters': '.*',
                            'data': 'var.my_dataset',
                            'stages': [
                                {
                                    'convert_list_to_string': {
                                        'source_key': 'tags',
                                        'separator': ', '
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }

        self.filters = {
            'accepted': '.*',
            'add_keys': ['test_add_key'],
            'exclude_keys': ['tags'],
            'headers': ['name', 'address.state', 'dob'],
            'matches': [['address.state=CA|TX']],
            'sort': ['name']
        }

    def assert_task_chain_success(self):
        # Verify the DataSetTask was instantiated into the TaskChain
        self.assertEqual(len(self.task_chain), 1)

        # Verify that the TaskChain completed without errors
        self.assertFalse(self.task_chain.errors)
        self.assertEqual(self.task_chain.status, TaskStatusCodes.complete)

    def test_task_chain(self):
        # Instantiate the TaskChain
        self.task_chain = task_chain_from_dict(template=self.task_chain_template, **{'filters': self.filters.copy()})

        # Set the my_dataset variable referenced in the task_chain_template.data
        self.task_chain.variables['my_dataset'] = self.dataset

        # Execute the TaskChain and validate it was successful
        self.task_chain.run()
        self.assert_task_chain_success()

        # Verify the records are properly filtered / processed
        self.assertEqual(len(self.task_chain.result['data']), 2)
        for record in self.task_chain.result['data']:
            self.assertNotIn('tags', record.keys())        # explicitly removed with 'exclude_keys'
            self.assertNotIn('email', record.keys())       # implicitly removed with 'headers'
            self.assertIn('test_add_key', record.keys())   # explicitly added with 'add_keys'

            # Verifies that the nested key value is not None
            for expected_key in ('name', 'address.state', 'dob'):
                self.assertIsNotNone(record.walk(expected_key))

        self.assertEqual(self.task_chain.result['data'][0]['name'], 'Jane Smith')
        self.assertEqual(self.task_chain.result['data'][0].walk('address.state'), 'TX')
        self.assertEqual(self.task_chain.result['data'][1]['name'], 'John Doe')
        self.assertEqual(self.task_chain.result['data'][1].walk('address.state'), 'CA')

    def test_task_chain_with_limited_filters(self):
        # In this test, we will only apply the 'add_keys' and 'sort' filters. Other filters will be ignored.
        template = self.task_chain_template.copy()
        template['report']['tasks'][0]['dataset']['filters'] = 'add_keys|sort'

        # Instantiate the TaskChain
        self.task_chain = task_chain_from_dict(template=self.task_chain_template, **{'filters': self.filters})

        # Set the my_dataset variable referenced in the task_chain_template.data
        self.task_chain.variables['my_dataset'] = self.dataset

        # Execute the TaskChain and validate it was successful
        self.task_chain.run()
        self.assert_task_chain_success()

        # Verify the records are properly filtered / processed
        self.assertEqual(len(self.task_chain.result['data']), 3)        # all records present because 'match' was not included
        for record in self.task_chain.result['data']:
            self.assertIn('test_add_key', record.keys())   # explicitly added with 'add_keys'

            # Verifies that the nested key value is not None
            for expected_key in template['report']['headers']:
                self.assertIsNotNone(record.walk(expected_key))

        self.assertEqual(self.task_chain.result['data'][0]['name'], 'Jane Smith')
        self.assertEqual(self.task_chain.result['data'][1]['name'], 'Jane Smith')
        self.assertEqual(self.task_chain.result['data'][2]['name'], 'John Doe')

    def test_task_chain_with_no_accepted_filters(self):
        # In this test, we will only apply the 'add_keys' and 'sort' filters. Other filters will be ignored.
        template = self.task_chain_template.copy()
        template['report']['tasks'][0]['dataset'].pop('filters')

        # Instantiate the TaskChain
        self.task_chain = task_chain_from_dict(template=self.task_chain_template, **{'filters': self.filters})

        # Set the my_dataset variable referenced in the task_chain_template.data
        self.task_chain.variables['my_dataset'] = self.dataset

        # Execute the TaskChain and validate it was successful
        self.task_chain.run()
        self.assert_task_chain_success()

        self.assertEqual(len(self.task_chain.result['data']), 3)
        for key in test_data[0].keys():
            for record in self.task_chain.result['data']:
                self.assertIn(key, record.keys())


class TestMongoTaskFilters(unittest.TestCase):
    def setUp(self):
        from CloudHarvestCoreTasks.silos import add_silo, get_silo
        add_silo(
            name='test',
            engine='mongo',
            host='localhost',
            port=27017,
            database='test',
            username='admin',
            password='default-harvest-password',
            authSource='admin',
            timeout=250
        )

        self.silo = get_silo('test')

        self.silo.connect()
        self.assertTrue(self.silo.is_connected)

        self.tearDown()

        # Upload the test data to a test collection
        client = self.silo.connect()
        client[self.silo.database]['test_collection'].insert_many(test_data)

        record_find_test = [record for record in client[self.silo.database]['test_collection'].find({})]

        self.assertEqual(len(record_find_test), len(test_data))

        self.task_chain_template = {
            'report': {
                'headers': ['name', 'address.state', 'dob'],
                'tasks': [
                    {
                        'mongo': {
                            'name': 'mongo task',
                            'description': 'Retrieve data from the Mongo database',
                            'collection': 'test_collection',
                            'filters': '.*',
                            'silo': 'test',
                            'command': 'aggregate',
                            'arguments': {
                                'pipeline': [

                                ]

                            }
                        }
                    }
                ]
            }
        }

        self.filters = {
            'accepted': '.*',
            'add_keys': ['test_add_key'],
            'exclude_keys': ['tags'],
            'headers': ['name', 'address.state', 'dob'],
            'matches': [['address.state=CA|TX']],
            'sort': ['name']
        }

    def tearDown(self):
        client = self.silo.connect()
        client[self.silo.database]['test_collection'].drop()

    def assert_task_chain_success(self):
        # Verify that the TaskChain completed without errors
        self.assertFalse(self.task_chain.errors)
        self.assertEqual(self.task_chain.status, TaskStatusCodes.complete)

    def test_task_chain(self):
        # Instantiate the TaskChain
        self.task_chain = task_chain_from_dict(template=self.task_chain_template, **{'filters': self.filters.copy()})

        # Execute the TaskChain and validate it was successful
        self.task_chain.run()
        self.assert_task_chain_success()

        # Verify the records are properly filtered / processed
        self.assertEqual(len(self.task_chain.result['data']), 2)
        for record in self.task_chain.result['data']:
            self.assertNotIn('tags', record.keys())        # explicitly removed with 'exclude_keys'
            self.assertNotIn('email', record.keys())       # implicitly removed with 'headers'

            # Verifies that the nested key value is not None
            for expected_key in ('name', 'address.state', 'dob'):
                self.assertIsNotNone(record.walk(expected_key))

        self.assertEqual(self.task_chain.result['data'][0]['name'], 'Jane Smith')
        self.assertEqual(self.task_chain.result['data'][0].walk('address.state'), 'TX')
        self.assertEqual(self.task_chain.result['data'][1]['name'], 'John Doe')
        self.assertEqual(self.task_chain.result['data'][1].walk('address.state'), 'CA')
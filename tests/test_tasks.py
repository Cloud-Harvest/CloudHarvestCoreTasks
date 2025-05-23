from datetime import datetime
import os
import tempfile
import unittest

from tasks.redis import unformat_hset
from tests.data import MONGO_TEST_RECORDS
from CloudHarvestCorePluginManager import register_all

from tasks.base import TaskStatusCodes
from CloudHarvestCoreTasks.factories import task_chain_from_dict
from CloudHarvestCoreTasks.tasks import (
    DummyTask,
    ErrorTask,
    FileTask,
    JsonTask,
    WaitTask
)

class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        register_all()
        super(BaseTestCase, cls).setUpClass()


class TestDummyTask(BaseTestCase):
    def setUp(self):
        self.dummy_task = DummyTask(name='dummy_task', description='This is a dummy task')

    def test_run(self):
        result = self.dummy_task.run()

        # Check that the method returns the instance of the DummyTask
        self.assertEqual(result, self.dummy_task)

        # Check that the dataset and meta attributes are set correctly
        self.assertEqual(self.dummy_task.result, [{'dummy': 'data'}])
        self.assertEqual(self.dummy_task.meta['info'], self.dummy_task.meta['info'])


class TestErrorTask(BaseTestCase):
    def setUp(self):
        self.error_task = ErrorTask(name='error_task', description='This is an error task')

    def test_run(self):
        from exceptions import TaskError

        try:
            self.error_task.run()

        except TaskError as ex:
            self.assertIsInstance(ex, TaskError)
            self.assertEqual(self.error_task.status, TaskStatusCodes.error)

class TestFileTask(BaseTestCase):
    def setUp(self):
        from chains.base import BaseTaskChain
        self.temp_files = []
        self.test_task_chain = BaseTaskChain(name='test_task_chain', description='This is a test task chain', template={'name': 'test', 'tasks': []})
        
        self.test_data = {
            'config': {'section': {'key': 'value'}},
            'csv': [{'key1': 'value1', 'key2': 'value2'}, {'key1': 'value3', 'key2': 'value4'}],
            'json': {'key1': 'value1', 'key2': 'value2'},
            'yaml': {'key1': 'value1', 'key2': 'value2'},
            'raw': 'This is raw dataset'
        }

    def tearDown(self):
        for file in self.temp_files:
            os.remove(file)

    def create_temp_file(self, content=''):
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_file.write(content.encode())
        temp_file.close()
        self.temp_files.append(temp_file.name)
        return temp_file.name

    def test_write_config(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain,
                        name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['config'],
                        mode='write',
                        format='config')
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertIn('[section]', content)
        self.assertIn('key = value', content)

    def test_read_config(self):
        path = self.create_temp_file('[section]\nkey = value\n')
        task = FileTask(name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['config'],
                        mode='read',
                        format='config')
        task.method()
        self.assertEqual(task.result, {'section': {'key': 'value'}})

    def test_write_csv(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain,
                        name="test",
                        path=path,
                        desired_keys=['key1', 'key2'],
                        result_as='result',
                        data=self.test_data['csv'],
                        mode='write',
                        format='csv')
        self.test_task_chain.variables = {'dataset': self.test_data['csv']}
        task.method()
        with open(path, 'r') as file:
            content = file.readlines()

        self.assertEqual(len(content), 3)
        self.assertEqual(content[0].strip(), 'key1,key2')
        self.assertEqual(content[1].strip(), 'value1,value2')
        self.assertEqual(content[2].strip(), 'value3,value4')

    def test_read_csv(self):
        path = self.create_temp_file('key1,key2\nvalue1,value2\nvalue3,value4\n')
        task = FileTask(name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['csv'],
                        mode='read',
                        format='csv')
        task.method()
        self.assertEqual(task.result, self.test_data['csv'])

    def test_write_json(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain,
                        name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['json'],
                        mode='write',
                        format='json')
        self.test_task_chain.variables = {'dataset': self.test_data['json']}
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertIn('"key1": "value1"', content)
        self.assertIn('"key2": "value2"', content)

    def test_read_json(self):
        path = self.create_temp_file('{"key1": "value1", "key2": "value2"}')
        task = FileTask(name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['json'],
                        mode='read',
                        format='json')
        task.method()
        self.assertEqual(task.result, self.test_data['json'])

    def test_write_yaml(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain,
                        name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['yaml'],
                        mode='write',
                        format='yaml')
        self.test_task_chain.variables = {'dataset': self.test_data['yaml']}
        task.method()

        from yaml import load, FullLoader
        with open(path, 'r') as file:
            content = load(file, Loader=FullLoader)

        [
            self.assertTrue(content[key] == self.test_data['yaml'][key])
            for key in self.test_data['yaml'].keys()
        ]

    def test_read_yaml(self):
        path = self.create_temp_file('key1: value1\nkey2: value2\n')
        task = FileTask(name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['yaml'],
                        mode='read',
                        format='yaml')
        task.method()
        [
            self.assertEqual(task.result[key], self.test_data['yaml'][key])
            for key in self.test_data['yaml'].keys()
        ]

    def test_write_raw(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain,
                        name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['raw'],
                        mode='write',
                        format='raw')
        self.test_task_chain.variables = {'dataset': self.test_data['raw']}
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertEqual(content, self.test_data['raw'])

    def test_read_raw(self):
        path = self.create_temp_file('This is raw dataset')
        task = FileTask(name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['raw'],
                        mode='read',
                        format='raw')
        task.method()
        self.assertEqual(task.result, 'This is raw dataset')


class TestDataSetTask(BaseTestCase):
    def setUp(self):
        # import required to register class

        harvest_dataset_task_template = {
            "name": "test_chain",
            "description": "This is a test chain.",
            "tasks": [
                {
                    "dataset": {
                        "name": "test dataset task",
                        "description": "This is a test record set",
                        "data": "var.test_dataset",
                        "stages": [
                            {
                                "convert_list_of_dict_to_dict": {
                                    "source_key": "tags",
                                    "target_key": "tags_dict",
                                    "name_key": "Name"
                                }
                            },
                            {
                                "copy_key": {
                                    "source_key": "age",
                                    "target_key": "age_copy"
                                }
                            }

                        ],
                        "results_as": "result"
                    }
                }

            ]

        }

        test_data = [
            {
                "name": "Test1",
                "age": 30,
                "date": datetime.now(),
                "tags": [{"Name": "color", "Value": "blue"}, {"Name": "size", "Value": "large"}]
            },
            {
                "name": "Test2",
                "age": 25,
                "date": datetime.now(),
                "tags": [{"Name": "color", "Value": "red"}, {"Name": "size", "Value": "medium"}]
            }
        ]

        from chains.base import BaseTaskChain
        self.test_data = test_data
        self.chain = BaseTaskChain(template=harvest_dataset_task_template)
        self.chain.variables["test_dataset"] = self.test_data

    def test_init(self):
        self.assertEqual(self.chain.variables["test_dataset"], self.test_data)
        self.assertEqual(self.chain.task_templates[0]['dataset']['name'], "test dataset task")

    def test_method(self):
        self.chain.run()
        result = self.chain.result
        self.assertEqual(result["data"][0]["tags_dict"], {"color": "blue", "size": "large"})
        self.assertEqual(result["data"][1]["tags_dict"], {"color": "red", "size": "medium"})
        [
            self.assertEqual(record["age"], record["age_copy"])
            for record in result["data"]
        ]

class TestHttpTask(BaseTestCase):
    def setUp(self):
        self.task_configuration = {
            'chain': {
                'name': 'Test Chain',
                'description': 'This is a test chain',
                'tasks': [
                    {
                        'http':
                            {
                                'name': 'Test HTTP Task',
                                'description': 'This is a test HTTP task',
                                'method': 'GET',
                                'url': 'https://127.0.0.1:8000/',
                                'verify': False
                            }
                    }
                ]
            }
        }

    def test_method(self):
        task_chain = task_chain_from_dict(template=self.task_configuration)
        task_chain.run()
        self.assertFalse(task_chain.errors)
        self.assertTrue(task_chain.result['data']['message'] == 'Welcome to the CloudHarvest API.')


class TestJsonTask(BaseTestCase):
    def setUp(self):
        import json
        self.now = datetime.now()

        self.test_deserialized_data = {
            "name": "Test1",
            "age": 30,
            "date": self.now,
            "tags": [{"Name": "color", "Value": "blue"}, {"Name": "size", "Value": "large"}]
        }

        self.test_serialized_json = json.dumps(self.test_deserialized_data, default=str)

    def test_method(self):
        # Test serialization
        task = JsonTask(name='test',
                        description='This is a test task',
                        data=self.test_deserialized_data,
                        mode='serialize')
        task.run()
        self.assertEqual(task.result, self.test_serialized_json)

        # Test deserialization
        task = JsonTask(name='test',
                        description='This is a test task',
                        data=self.test_serialized_json,
                        mode='deserialize',
                        parse_datetimes=True)
        task.run()
        self.assertEqual(task.result, self.test_deserialized_data)

class TestMongoTask(BaseTestCase):
    def setUp(self):
        from CloudHarvestCoreTasks.silos import add_silo, get_silo

        add_silo(name='test_silo',
                 engine='mongo',
                 host='localhost',
                 port=27017,
                 username='harvest-api',
                 password='default-harvest-password',
                 database='harvest',
                 authSource='harvest')

        from pymongo import MongoClient
        silo = get_silo('test_silo')
        client: MongoClient = silo.connect()
        self.collection = client['harvest']['users']

        # Ensures that the collection is empty before inserting the records
        self.collection.drop()

        self.collection.insert_many(MONGO_TEST_RECORDS)
        assert len(list(self.collection.find())) == 10

    def tearDown(self):
        self.collection.drop()

        assert len(list(self.collection.find())) == 0

    def test_method_find(self):
        task_chain_configuration = {
            'report': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'mongo': {
                            'name': 'find test',
                            'silo': 'test_silo',
                            'collection': 'users',
                            'result_as': 'mongo_result',
                            'command': 'find',
                            'arguments': {
                                'filter': {}
                            },

                        }
                    }
                ]
            }
        }

        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        self.assertFalse(task_chain.errors)
        self.assertEqual(len(task_chain.result['data']), 10)

    # def test_method_subcommand(self):
    #     """
    #     This test checks if commands like 'find.explain()' return the expected result.
    #     """
    #
    #     task_chain_configuration = {
    #         'chain': {
    #             'name': 'test_chain',
    #             'tasks': [
    #                 {
    #                     'mongo': {
    #                         'name': 'find.explain test',
    #                         'silo': 'test_silo',
    #                         'collection': 'users',
    #                         'result_as': 'mongo_result',
    #                         'command': 'find.explain',
    #                         'arguments': {
    #                             'filter': {}
    #                         }
    #                      }
    #                 }
    #             ]
    #         }
    #     }
    #
    #     task_chain = task_chain_from_dict(template=task_chain_configuration)
    #     task_chain.run()
    #
    #     self.assertFalse(task_chain.errors)
    #     self.assertIn('command', task_chain.result['data'].keys())


class TestRedisTask(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        from CloudHarvestCoreTasks.silos import add_silo

        add_silo(name='test_silo',
                 engine='redis',
                 host='localhost',
                 port=6379,
                 database=10,
                 password='default-harvest-password',
                 decode_responses=True)


        from redis import StrictRedis
        cls.redis_connection_config = {
            'host': 'localhost',
            'port': 6379,
            'password': 'default-harvest-password',
            'decode_responses': True
        }

        cls.connection = StrictRedis(**cls.redis_connection_config)

    def tearDown(self):
        self.connection.flushall()

    def test_hash_get_and_set(self):
        from CloudHarvestCoreTasks.tasks.redis import RedisTask

        from uuid import uuid4
        record_name = str(uuid4())
        data = {
            'redis_name': record_name,
            'name': {
                'first': 'John',
                'last': 'Doe'
            },
            'dob': '1990-01-01',
            'age': 30,
            'city': 'New York',
            'state': 'NY',
            'country': 'USA',
            'tags': ['tag1', 'tag2', 'tag3'],
        }

        set_task = RedisTask(
            name='test_hash_set',
            description='This is a test Redis hash set task',
            silo='test_silo',
            command='hset',
            arguments={
                'name': record_name,
                'mapping': data
            },
            serializer='hset',          # will make sure keys are preserved but list data (like 'tags') are converted to strings
            serializer_key='mapping',
        )

        set_task.run()

        self.assertFalse(set_task.errors)

        from CloudHarvestCoreTasks.silos import get_silo
        silo = get_silo('test_silo')

        # Check that the data was set correctly
        from CloudHarvestCoreTasks.tasks.redis import unformat_hset
        redis_data = unformat_hset(silo.connect().hgetall(record_name))
        self.assertEqual(redis_data['name'], data['name'])
        self.assertEqual(redis_data['dob'], data['dob'])
        self.assertEqual(redis_data['age'], data['age'])
        self.assertEqual(redis_data['city'], data['city'])
        self.assertEqual(redis_data['state'], data['state'])
        self.assertEqual(redis_data['country'], data['country'])
        self.assertEqual(redis_data['tags'], data['tags'])


        desired_keys = ['name', 'dob', 'age', 'city', 'state', 'country', 'tags']
        get_task = RedisTask(
            name='test_hash_get',
            description='This is a test Redis hash get task',
            silo='test_silo',
            command='hmget',
            arguments={
                'name': record_name,
                'keys': desired_keys
            },
            serializer='hget',
            rekey=True
        )

        get_task.run()
        self.assertFalse(get_task.errors)
        for key in desired_keys:
            self.assertIn(key, get_task.result.keys())
            self.assertEqual(get_task.result[key], data[key])

class TestPruneTask(BaseTestCase):
    def setUp(self):
        """
        Set up the test environment for each test case.
        """

        # Create a dummy task and add it to the task chain
        self.task_configuration = {
            'chain': {
                'name': 'test_chain',
                'description': 'This is a task_chain.',
                'tasks': [
                    {
                        'dummy': {
                            'name': 'dummy_task-1',
                            'description': 'This is a dummy task'
                        }
                    },
                    {
                        'dummy': {
                            'name': 'dummy_task-2',
                            'description': 'This is a dummy task'
                        }
                    },
                    {
                        'dummy': {
                            'name': 'dummy_task-3',
                            'description': 'This is a dummy task'
                        }
                    },
                    {
                        'prune': {
                            'name': 'prune_task',
                            'description': 'This is a prune task',
                            'previous_task_data': True
                        }
                    }
                ]
            }
        }

    def test_run_prune_all_at_end(self):
        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='report',
                                               template=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the result attribute of each task in the task chain is None
        self.assertGreaterEqual(self.task_chain[-1].result.get('total_bytes_pruned'), 0)
        [self.assertIsNone(task.result) for task in self.task_chain[0:-1]]

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(str(self.task_chain.status), TaskStatusCodes.complete)

    def test_run_prune_all_at_near_end(self):
        self.task_configuration['chain']['tasks'].append({
            'dummy': {
                'name': 'dummy_task-4',
                'description': 'This is a dummy task after a prune',
            }
        })

        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='report',
                                               template=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the dataset attribute of each task in the task before the PruneTask is None
        self.assertGreater(self.task_chain[3].result.get('total_bytes_pruned'), 0)
        [
            self.assertIsNone(task.result)
            for task in self.task_chain[0:3]
        ]
        self.assertEqual(self.task_chain[4].result, [{'dummy': 'data'}])
        self.assertEqual(self.task_chain[4].meta['info'], 'this is dummy metadata')

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(str(self.task_chain.status), TaskStatusCodes.complete)

class TestWaitTask(BaseTestCase):
    def setUp(self):
        self.task = WaitTask(name='wait_task', description='This is a wait task', when_after_seconds=5)

    def test_run(self):
        from datetime import datetime, timedelta
        start_time = datetime.now()
        self.task.run()
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        self.assertTrue(timedelta(seconds=4.5) < elapsed_time < timedelta(seconds=5.5))


if __name__ == '__main__':
    unittest.main()

from datetime import datetime
import os
import tempfile
import unittest

from ..CloudHarvestCoreTasks.tasks import *


class TestDummyTask(unittest.TestCase):
    def setUp(self):
        self.dummy_task = DummyTask(name='dummy_task', description='This is a dummy task')

    def test_run(self):
        result = self.dummy_task.run()

        # Check that the method returns the instance of the DummyTask
        self.assertEqual(result, self.dummy_task)

        # Check that the data and meta attributes are set correctly
        self.assertEqual(self.dummy_task.result, [{'dummy': 'data'}])
        self.assertEqual(self.dummy_task.meta['info'], self.dummy_task.meta['info'])


class TestErrorTask(unittest.TestCase):
    def setUp(self):
        self.error_task = ErrorTask(name='error_task', description='This is an error task')

    def test_run(self):
        self.error_task.run()
        self.assertEqual(str(self.error_task.status), str(TaskStatusCodes.error))

class TestFileTask(unittest.TestCase):
    def setUp(self):
        from ..CloudHarvestCoreTasks.tasks.base import BaseTaskChain
        self.temp_files = []
        self.test_task_chain = BaseTaskChain(name='test_task_chain', description='This is a test task chain', template={'name': 'test', 'tasks': []})
        
        self.test_data = {
            'config': {'section': {'key': 'value'}},
            'csv': [{'key1': 'value1', 'key2': 'value2'}, {'key1': 'value3', 'key2': 'value4'}],
            'json': {'key1': 'value1', 'key2': 'value2'},
            'yaml': {'key1': 'value1', 'key2': 'value2'},
            'raw': 'This is raw data'
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
        self.test_task_chain.variables = {'data': self.test_data['csv']}
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
        self.test_task_chain.variables = {'data': self.test_data['json']}
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
        self.test_task_chain.variables = {'data': self.test_data['yaml']}
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
        self.test_task_chain.variables = {'data': self.test_data['raw']}
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertEqual(content, self.test_data['raw'])

    def test_read_raw(self):
        path = self.create_temp_file('This is raw data')
        task = FileTask(name="test",
                        path=path,
                        result_as='result',
                        data=self.test_data['raw'],
                        mode='read',
                        format='raw')
        task.method()
        self.assertEqual(task.result, 'This is raw data')


class TestHarvestRecordSetTask(unittest.TestCase):
    def setUp(self):
        # import required to register class

        harvest_recordset_task_template = {
            "name": "test_chain",
            "description": "This is a test chain.",
            "tasks": [
                {
                    "recordset": {
                        "name": "test recordset task",
                        "description": "This is a test record set",
                        "data": "var.test_recordset",
                        "stages": [
                            {
                                "key_value_list_to_dict": {
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

        from ..CloudHarvestCoreTasks.tasks.base import BaseTaskChain
        self.test_data = test_data
        self.chain = BaseTaskChain(template=harvest_recordset_task_template)
        self.chain.variables["test_recordset"] = self.test_data

    def test_init(self):
        self.assertEqual(self.chain.variables["test_recordset"], self.test_data)
        self.assertEqual(self.chain.task_templates[0]['recordset']['name'], "test recordset task")

    def test_method(self):
        self.chain.run()
        result = self.chain.result
        self.assertEqual(result["data"][0]["tags_dict"], {"color": "blue", "size": "large"})
        self.assertEqual(result["data"][1]["tags_dict"], {"color": "red", "size": "medium"})
        [
            self.assertEqual(record["age"], record["age_copy"])
            for record in result["data"]
        ]


class TestJsonTask(unittest.TestCase):
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
        from ..CloudHarvestCoreTasks.tasks import JsonTask

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

class TestMongoTask(unittest.TestCase):
    def setUp(self):
        self.database_connection_config = {
            'host': 'localhost',
            'port': 44444,
            'username': 'admin',
            'password': 'default-harvest-password',
            'database': 'harvest'
        }

    def test_init(self):
        from ..CloudHarvestCoreTasks.tasks import MongoTask

        # Assert that the task is not created if the database parameters are missing
        self.assertRaises(ValueError,
                          MongoTask,
                          name='test',
                          description='This is a test task',
                          alias='invalid',
                          command='find',
                          collection='test')

        # Assert that the task is created
        mongo_task = MongoTask(name='test',
                               description='This is a test task',
                               collection='test',
                               command='test',
                               host='localhost',
                               port=27017,
                               database='test')

        self.assertTrue(mongo_task)

    def test_method_find(self):
        task_chain_configuration = {
            'report': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'mongo': {
                            'name': 'find test',
                            'collection': 'users',
                            'result_as': 'mongo_result',
                            'command': 'find',
                            'arguments': {
                                'filter': {}
                            },

                        } | self.database_connection_config,
                    }
                ]
            }
        }

        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        self.assertEqual(len(task_chain.result['data']), 10)

    def test_method_subcommand(self):
        """
        This test checks if commands like 'find.explain()' return the expected result.
        """

        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'mongo': {
                            'name': 'find.explain test',
                            'collection': 'users',
                            'result_as': 'mongo_result',
                            'command': 'find.explain',
                            'arguments': {
                                'filter': {}
                            }
                         } | self.database_connection_config
                    }
                ]
            }
        }

        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        self.assertIn('command', task_chain.result['data'].keys())

    def test_connection_from_silo(self):
        from ..CloudHarvestCoreTasks.silos import add_silo

        add_silo(name='test_silo', **self.database_connection_config | {'engine': 'mongo'})

        task_chain_configuration = {
            'report': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'mongo': {
                            'name': 'find test',
                            'collection': 'users',
                            'result_as': 'mongo_result',
                            'silo': 'test_silo',
                            'command': 'find',
                            'arguments': {
                                'filter': {}
                            },
                        }
                    }
                ]
            }
        }

        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        self.assertFalse(task_chain.errors)
        self.assertEqual(len(task_chain.result['data']), 10)

class TestRedisTask(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from redis import StrictRedis
        cls.redis_connection_config = {
            'host': 'localhost',
            'port': 44445,
            'password': 'default-harvest-password',
            'decode_responses': True
        }

        cls.connection = StrictRedis(**cls.redis_connection_config)

    def tearDown(self):
        self.connection.flushall()

    def test_redis_delete(self):
        self.connection.set('key1', 'value1')
        self.connection.set('key2', 'value2')

        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'delete test',
                                     'command': 'delete',
                                     'arguments': {'keys': ['key1', 'key2']},
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        self.assertEqual(result['data']['deleted'], 2)
        self.assertEqual(result['data']['keys'], ['key1', 'key2'])

    def test_redis_expire(self):
        self.connection.set('key1', 'value1')
        self.connection.set('key2', 'value2')

        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'expire test',
                                     'command': 'expire',
                                     'arguments': {'expire': 3600, 'keys': ['key1', 'key2']},
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        self.assertEqual(result['data'], ['key1', 'key2'])
        self.assertTrue(self.connection.ttl('key1') > 0)
        self.assertTrue(self.connection.ttl('key2') > 0)

    def test_redis_flushall(self):
        self.connection.set('key1', 'value1')

        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'flushall test',
                                     'command': 'flushall',
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        self.assertTrue(result['data']['deleted'])
        self.assertEqual(self.connection.keys('*'), [])

    def test_redis_get(self):
        self.connection.set('test_key', 'test_value')

        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'get test',
                                     'command': 'get',
                                     'arguments': {'name': 'test_key'},
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        self.assertEqual(result['data'], [{'test_key': 'test_value'}])

    def test_redis_keys(self):
        self.connection.set('key1', 'value1')
        self.connection.set('key2', 'value2')

        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'keys test',
                                     'command': 'keys',
                                     'arguments': {'pattern': 'key*'},
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        self.assertEqual(set(result['data']), {'key1', 'key2'})

    def test_redis_set(self):
        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'set test',
                                     'command': 'set',
                                     'arguments': {
                                         'name': 'test_key',
                                         'value': 'test_value'
                                     },
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        self.assertEqual(result['data']['added'], 1)
        self.assertEqual(result['data']['errors'], 0)
        self.assertEqual(result['data']['updated'], 0)
        self.assertEqual(self.connection.get('test_key'), 'test_value')

    def test_redis_serialize_set(self):
        nested_dict = {'key1': {'subkey1': 'value1', 'subkey2': 'value2'}}
        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'serialize set test',
                                     'command': 'set',
                                     'serialization': True,
                                     'arguments': {
                                         'name': 'test_key',
                                         'value': nested_dict
                                     },
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.run()

        result = task_chain.result
        serialized_value = self.connection.get('test_key')
        self.assertEqual(result['data']['added'], 1)
        self.assertEqual(serialized_value, '{"key1": {"subkey1": "value1", "subkey2": "value2"}}')

    def test_redis_set_dict(self):
        dict_to_set = {'field1': 'value1', 'field2': 'value2'}
        task_chain_configuration = {
            'chain': {
                'name': 'test_chain',
                'tasks': [
                    {
                        'redis': {
                                     'name': 'set dict test',
                                     'command': 'set',
                                     'data': 'var.dict_to_set',
                                     'arguments': {
                                         'name': 'test_hash',
                                         'keys': [
                                             'field1',
                                             'field2'
                                         ]
                                     },
                                 } | self.redis_connection_config,
                    }
                ]
            }
        }
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        task_chain = task_chain_from_dict(template=task_chain_configuration)
        task_chain.variables['dict_to_set'] = dict_to_set

        task_chain.run()

        result = task_chain.result
        self.assertEqual(result['data']['added'], 1)
        self.assertEqual(result['data']['errors'], 0)
        self.assertEqual(result['data']['updated'], 0)
        self.assertEqual(self.connection.hget('test_hash', 'field1'), 'value1')
        self.assertEqual(self.connection.hget('test_hash', 'field2'), 'value2')

class TestPruneTask(unittest.TestCase):
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
        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='report',
                                               template=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the result attribute of each task in the task chain is None
        self.assertGreaterEqual(self.task_chain[-1].result.get('total_bytes_pruned'), 0)
        [self.assertIsNone(task.result) for task in self.task_chain[0:-1]]

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(str(self.task_chain.status), str(TaskStatusCodes.complete))

    def test_run_prune_all_at_near_end(self):
        self.task_configuration['chain']['tasks'].append({
            'dummy': {
                'name': 'dummy_task-4',
                'description': 'This is a dummy task after a prune',
            }
        })

        from ..CloudHarvestCoreTasks.tasks.factories import task_chain_from_dict
        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='report',
                                               template=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the data attribute of each task in the task before the PruneTask is None
        self.assertGreater(self.task_chain[3].result.get('total_bytes_pruned'), 0)
        [
            self.assertIsNone(task.result)
            for task in self.task_chain[0:3]
        ]
        self.assertEqual(self.task_chain[4].result, [{'dummy': 'data'}])
        self.assertEqual(self.task_chain[4].meta['info'], 'this is dummy metadata')

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(str(self.task_chain.status), str(TaskStatusCodes.complete))

class TestWaitTask(unittest.TestCase):
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

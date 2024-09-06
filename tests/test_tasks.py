import os
import tempfile
import unittest
from ..CloudHarvestCoreTasks.__register__ import *
from ..CloudHarvestCoreTasks.base import TaskStatusCodes


class TestDummyTask(unittest.TestCase):
    def setUp(self):
        self.dummy_task = DummyTask(name='dummy_task', description='This is a dummy task')

    def test_run(self):
        result = self.dummy_task.run()

        # Check that the method returns the instance of the DummyTask
        self.assertEqual(result, self.dummy_task)

        # Check that the data and meta attributes are set correctly
        self.assertEqual(self.dummy_task.out_data, [{'dummy': 'data'}])
        self.assertEqual(self.dummy_task.meta, {'info': 'this is dummy metadata'})


class TestFileTask(unittest.TestCase):
    def setUp(self):
        from ..CloudHarvestCoreTasks.base import BaseTaskChain
        self.temp_files = []
        self.test_task_chain = BaseTaskChain(name='test_task_chain', description='This is a test task chain', template={'name': 'test', 'tasks': []})

        from collections import OrderedDict
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
                        with_vars=['data'],
                        mode='write',
                        format='config')
        self.test_task_chain.variables = {'data': self.test_data['config']}
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertIn('[section]', content)
        self.assertIn('key = value', content)

    def test_read_config(self):
        path = self.create_temp_file('[section]\nkey = value\n')
        task = FileTask(name="test", path=path, result_as='result', with_vars=['data'], mode='read', format='config')
        task.method()
        self.assertEqual(task.out_data, {'section': {'key': 'value'}})

    def test_write_csv(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain,
                        name="test",
                        path=path,
                        desired_keys=['key1', 'key2'],
                        result_as='result',
                        with_vars=['data'],
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
        task = FileTask(name="test", path=path, result_as='result', with_vars=['data'], mode='read', format='csv')
        task.method()
        self.assertEqual(task.out_data, self.test_data['csv'])

    def test_write_json(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain, name="test", path=path, result_as='result', with_vars=['data'], mode='write', format='json')
        self.test_task_chain.variables = {'data': self.test_data['json']}
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertIn('"key1": "value1"', content)
        self.assertIn('"key2": "value2"', content)

    def test_read_json(self):
        path = self.create_temp_file('{"key1": "value1", "key2": "value2"}')
        task = FileTask(name="test", path=path, result_as='result', with_vars=['data'], mode='read', format='json')
        task.method()
        self.assertEqual(task.out_data, self.test_data['json'])

    def test_write_yaml(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain, name="test", path=path, result_as='result', with_vars=['data'], mode='write', format='yaml')
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
                        with_vars=['data'],
                        mode='read',
                        format='yaml')
        task.method()
        [
            self.assertEqual(task.out_data[key], self.test_data['yaml'][key])
            for key in self.test_data['yaml'].keys()
        ]

    def test_write_raw(self):
        path = self.create_temp_file()
        task = FileTask(task_chain=self.test_task_chain, name="test", path=path, result_as='result', with_vars=['data'], mode='write', format='raw')
        self.test_task_chain.variables = {'data': self.test_data['raw']}
        task.method()
        with open(path, 'r') as file:
            content = file.read()
        self.assertEqual(content, self.test_data['raw'])

    def test_read_raw(self):
        path = self.create_temp_file('This is raw data')
        task = FileTask(name="test", path=path, result_as='result', with_vars=['data'], mode='read', format='raw')
        task.method()
        self.assertEqual(task.out_data, 'This is raw data')


class TestPruneTask(unittest.TestCase):
    def setUp(self):
        """
        Set up the test environment for each test case.
        """

        # Create a dummy task and add it to the task chain
        self.task_configuration = {
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

    def test_run_prune_all_at_end(self):
        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='report',
                                               task_chain=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the out_data attribute of each task in the task chain is None
        self.assertGreaterEqual(self.task_chain[-1].out_data.get('total_bytes_pruned'), 0)
        [self.assertIsNone(task.out_data) for task in self.task_chain[0:-1]]

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(self.task_chain.status, TaskStatusCodes.complete)

    def test_run_prune_all_at_near_end(self):
        self.task_configuration['tasks'].append({
            'dummy': {
                'name': 'dummy_task-4',
                'description': 'This is a dummy task after a prune',
            }
        })

        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='report',
                                               task_chain=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the data attribute of each task in the task before the PruneTask is None
        self.assertGreater(self.task_chain[3].out_data.get('total_bytes_pruned'), 0)
        [self.assertIsNone(task.out_data) for task in self.task_chain[0:3]]
        self.assertEqual(self.task_chain[4].out_data, [{'dummy': 'data'}])
        self.assertEqual(self.task_chain[4].meta, {'info': 'this is dummy metadata'})

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(self.task_chain.status, TaskStatusCodes.complete)


class TestForEachTask(unittest.TestCase):
    def setUp(self):
        task_chain_configuration = {
            'name': 'test_chain',
            'description': 'This is a task_chain.',
            'tasks': [
                {
                    'dummy': {
                        'name': 'Control Task',
                        'description': 'This is a control task which should always succeed',
                    }
                },
                {
                    'for_each': {
                        'name': 'For Each Task',
                        'description': 'This task will create many new tasks based on the content of a variable.',
                        'insert_tasks_at_position': 2,
                        'template': {
                            'file': {
                                'name': 'File Task {{ name }}',
                                'description': 'This is a file task for record {{ name }}',
                                'mode': 'write',
                                'path': '/tmp/{{ name }}.json',
                            }
                        },
                        'in_data': 'i'
                    }
                }
            ]
        }

        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.task_chain = task_chain_from_dict(task_chain_registered_class_name='chain',
                                               task_chain=task_chain_configuration)

        self.task_chain.variables = {'i': [{'name': 1}, {'name': 2}, {'name': 3}]}

    def tearDown(self):
        import os
        for record in self.task_chain.variables['i']:
            file_path = f"/tmp/{record['name']}.json"
            if os.path.exists(file_path):
                os.remove(file_path)

    def test_for_each_task_generation(self):
        self.task_chain.run()

        # Check that the task chain has the correct number of tasks: Control, ForEach, and 3 generated Dummy tasks
        self.assertEqual(5, len(self.task_chain))

        # Check that all Dummy tasks have been created
        self.assertTrue(all([
            self.task_chain.find_task_by_name(f'File Task {i["name"]}')
            for i in self.task_chain.variables.get('i')
        ]))


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

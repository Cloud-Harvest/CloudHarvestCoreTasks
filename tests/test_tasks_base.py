import unittest
from unittest.mock import patch
from ..CloudHarvestCoreTasks.tasks import *
from ..CloudHarvestCoreTasks.__register__ import *
from CloudHarvestCorePluginManager.registry import Registry


class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Registry.register_objects()
        super(BaseTestCase, cls).setUpClass()


class TestTaskStatusCodes(BaseTestCase):
    def test_enum_values(self):
        self.assertEqual(TaskStatusCodes.complete.value, 'complete')
        self.assertEqual(TaskStatusCodes.error.value, 'error')
        self.assertEqual(TaskStatusCodes.initialized.value, 'initialized')
        self.assertEqual(TaskStatusCodes.running.value, 'running')
        self.assertEqual(TaskStatusCodes.terminating.value, 'terminating')


class TestTaskConfiguration(BaseTestCase):
    def setUp(self):
        # Create a dummy task and add it to the registry
        self.task_configuration = {
                'name': 'test_chain',
                'description': 'This is a task_chain.',
                'tasks': [

                    {
                        'dummy': {
                            'name': 'dummy_task',
                            'description': 'This is a dummy task'
                        }
                    },
                    {
                        'delay': {
                            'name': 'delay_task',
                            'description': 'This is a delay task',
                            'delay_seconds': 1
                        }
                    }
                ]
            }

        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.base_task_chain = task_chain_from_dict(task_chain_registered_class_name='report', task_chain=self.task_configuration)

    def test_instantiate(self):
        # Test the instantiate method
        self.base_task_chain.run()
        self.assertIsNone(self.base_task_chain.result.get('error'))
        self.assertIsInstance(self.base_task_chain[0], DummyTask)
        self.assertIsInstance(self.base_task_chain[1], DelayTask)
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.complete)


class TestBaseTask(BaseTestCase):
    def setUp(self):
        from CloudHarvestCorePluginManager.registry import Registry
        task = Registry.find_definition(class_name='delay', is_subclass_of=BaseTask)[0]
        self.base_task = task(name='test', description='test task', delay_seconds=10)

    def test_init(self):
        # Test the __init__ method
        self.assertEqual(self.base_task.name, 'test')
        self.assertEqual(self.base_task.description, 'test task')
        self.assertEqual(self.base_task.status, TaskStatusCodes.initialized)

    def test_run(self):
        # Test the run method
        self.base_task.run()
        self.assertEqual(self.base_task.status, TaskStatusCodes.complete)

    def test_on_complete(self):
        # Test the on_complete method
        self.base_task.on_complete()
        self.assertEqual(self.base_task.status, TaskStatusCodes.complete)

    def test_on_error(self):
        # Test the on_error method
        try:
            raise Exception('Test exception')
        except Exception as e:
            self.base_task.on_error(e)
        self.assertEqual(self.base_task.status, TaskStatusCodes.error)

    def test_terminate(self):
        # Test the terminate method
        self.base_task.terminate()
        self.assertEqual(self.base_task.status, TaskStatusCodes.terminating)


class TestBaseAsyncTask(BaseTestCase):
    def setUp(self):
        self.base_async_task = BaseAsyncTask(name='test', description='test task')

    def test_init(self):
        # Test the __init__ method
        self.assertEqual(self.base_async_task.name, 'test')
        self.assertEqual(self.base_async_task.description, 'test task')
        self.assertEqual(self.base_async_task.status, TaskStatusCodes.initialized)
        self.assertIsNone(self.base_async_task.thread)

    @patch('threading.Thread')
    def test_run(self, mock_thread):
        # Test the run method
        self.base_async_task.run()
        self.assertEqual(self.base_async_task.status, TaskStatusCodes.running)
        mock_thread.assert_called_once()

    @patch('threading.Thread')
    def test_terminate(self, mock_thread):
        # Test the terminate method
        self.base_async_task.run()
        self.base_async_task.terminate()
        self.assertEqual(self.base_async_task.status, TaskStatusCodes.terminating)
        mock_thread.return_value.join.assert_called_once()


class TestBaseTaskChain(BaseTestCase):
    """
    Unit tests for the BaseTaskChain class.
    """

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
                            'name': 'dummy_task',
                            'description': 'This is a dummy task'
                        }
                    },
                    {
                        'delay': {
                            'name': 'delay_task',
                            'description': 'This is a delay task',
                            'delay_seconds': 1
                        }
                    }
                ]
            }

        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.base_task_chain = task_chain_from_dict(task_chain_registered_class_name='report', task_chain=self.task_configuration)

    def test_init(self):
        """
        Test the __init__ method of the BaseTaskChain class.
        """
        # Assert that the initial values of the task chain attributes are as expected
        self.assertEqual(self.base_task_chain.name, 'test_chain')
        self.assertEqual(self.base_task_chain.description, 'This is a task_chain.')
        self.assertEqual(self.base_task_chain.variables, {})
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.initialized)
        self.assertEqual(self.base_task_chain.position, 0)
        self.assertEqual(self.base_task_chain.start, None)
        self.assertEqual(self.base_task_chain.end, None)
        self.assertEqual(self.base_task_chain.result.get('meta'), None)

    async def test_run(self):
        """
        Test the run method of the BaseTaskChain class.
        """

        from asyncio import create_task

        # Run the task chain
        self.base_task_chain.run()

        # Assert that the status of the task chain is 'complete'
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.complete)

    def test_on_complete(self):
        """
        Test the on_complete method of the BaseTaskChain class.
        """
        # Call the on_complete method of the task chain
        self.base_task_chain.on_complete()
        # Assert that the status of the task chain is 'complete'
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.complete)

    def test_on_error(self):
        """
        Test the on_error method of the BaseTaskChain class.
        """
        # Simulate an error
        try:
            raise Exception('Test exception')
        except Exception as e:
            # Call the on_error method of the task chain
            self.base_task_chain.on_error(e)
        # Assert that the status of the task chain is 'error'
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.error)

    def test_terminate(self):
        """
        Test the terminate method of the BaseTaskChain class.
        """
        # Call the terminate method of the task chain
        self.base_task_chain.terminate()
        # Assert that the status of the task chain is 'terminating'
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.terminating)

    def test_performance_metrics(self):
        """
        Test the performance_metric method of the BaseTaskChain class.
        """
        self.base_task_chain.run()
        report = self.base_task_chain.performance_metrics

        # Assert that the report is a dictionary
        self.assertIsInstance(report, list)
        # Assert that the report contains the expected keys
        self.assertEqual(report[0]['data'][-2]['Position'], '')
        self.assertEqual(report[0]['data'][-1]['Position'], 'Total')


if __name__ == '__main__':
    unittest.main()

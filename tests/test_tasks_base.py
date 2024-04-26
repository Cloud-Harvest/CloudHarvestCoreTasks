import unittest
from unittest.mock import patch
from tasks.base import *


class TestTaskStatusCodes(unittest.TestCase):
    def test_enum_values(self):
        self.assertEqual(TaskStatusCodes.complete.value, TaskStatusCodes.complete)
        self.assertEqual(TaskStatusCodes.error.value, TaskStatusCodes.error)
        self.assertEqual(TaskStatusCodes.initialized.value, 'initialized')
        self.assertEqual(TaskStatusCodes.running.value, TaskStatusCodes.running)
        self.assertEqual(TaskStatusCodes.terminating.value, TaskStatusCodes.terminating)


class TestTaskRegistry(unittest.TestCase):
    def setUp(self):
        # Create a dummy task and add it to the registry
        class DummyTask(BaseTask):
            pass

        self.dummy_task = DummyTask(name='dummy')

    def test_task_class_by_name(self):
        # Test the get_task_class_by_name method
        task_class = TaskRegistry.get_task_class_by_name('dummy')
        self.assertEqual(task_class, self.dummy_task.__class__)


class TestTaskConfiguration(unittest.TestCase):
    def setUp(self):
        # Create a dummy task and add it to the registry
        class DummyTask(BaseTask):
            pass

        self.dummy_task = DummyTask(name='dummy')
        TaskRegistry.tasks['dummy'] = DummyTask

        self.task_configuration = {
            'dummy': {
                'name': 'dummy_task',
                'description': 'This is a dummy task'
            }
        }

        self.task_config_instance = TaskConfiguration(task_configuration=self.task_configuration)

    def test_instantiate(self):
        # Test the instantiate method
        instantiated_task = self.task_config_instance.instantiate()
        self.assertIsInstance(instantiated_task, BaseTask)
        self.assertEqual(instantiated_task.name, 'dummy_task')
        self.assertEqual(instantiated_task.description, 'This is a dummy task')


class TestBaseTask(unittest.TestCase):
    def setUp(self):
        self.base_task = BaseTask(name='test', description='test task')

    def test_init(self):
        # Test the __init__ method
        self.assertEqual(self.base_task.name, 'test')
        self.assertEqual(self.base_task.description, 'test task')
        self.assertEqual(self.base_task.status, TaskStatusCodes.initialized)

    def test_run(self):
        # Test the run method
        self.base_task.run()
        self.assertEqual(self.base_task.status, TaskStatusCodes.initialized)

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


class TestBaseAsyncTask(unittest.TestCase):
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


class TestBaseTaskChain(unittest.TestCase):
    """
    Unit tests for the BaseTaskChain class.
    """

    def setUp(self):
        """
        Set up the test environment for each test case.
        """
        # Create a dummy task and add it to the task chain
        class DummyTask(BaseTask):
            pass

        self.dummy_task = DummyTask(name='dummy')
        self.task_configuration = {'dummy': {'name': 'dummy_task', 'description': 'This is a dummy task'}}
        self.base_task_chain = BaseTaskChain(name='test_chain', template=self.task_configuration)

    def test_init(self):
        """
        Test the __init__ method of the BaseTaskChain class.
        """
        # Assert that the initial values of the task chain attributes are as expected
        self.assertEqual(self.base_task_chain.name, 'test_chain')
        self.assertEqual(self.base_task_chain.description, None)
        self.assertEqual(self.base_task_chain.variables, {})
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.initialized)
        self.assertEqual(self.base_task_chain.position, 0)
        self.assertEqual(self.base_task_chain.start, None)
        self.assertEqual(self.base_task_chain.end, None)
        self.assertEqual(self.base_task_chain.meta, None)

    def test_run(self):
        """
        Test the run method of the BaseTaskChain class.
        """
        # Run the task chain
        self.base_task_chain.run()
        # Assert that the status of the task chain is 'running'
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.running)

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


if __name__ == '__main__':
    unittest.main()

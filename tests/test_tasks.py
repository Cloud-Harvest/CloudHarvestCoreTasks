import unittest
from ..CloudHarvestCoreTasks.__register__ import *
from ..CloudHarvestCoreTasks.base import TaskStatusCodes


class TestDelayTask(unittest.TestCase):
    def setUp(self):
        self.delay_task = DelayTask(name='delay_task', description='This is a delay task', delay_seconds=5)

    def test_run(self):
        from datetime import datetime, timedelta
        start_time = datetime.now()
        self.delay_task.run()
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        self.assertTrue(timedelta(seconds=4.5) < elapsed_time < timedelta(seconds=5.5))


class TestDummyTask(unittest.TestCase):
    def setUp(self):
        self.dummy_task = DummyTask(name='dummy_task', description='This is a dummy task')

    def test_run(self):
        result = self.dummy_task.run()

        # Check that the method returns the instance of the DummyTask
        self.assertEqual(result, self.dummy_task)

        # Check that the data and meta attributes are set correctly
        self.assertEqual(self.dummy_task.data, [{'dummy': 'data'}])
        self.assertEqual(self.dummy_task.meta, {'info': 'this is dummy metadata'})


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
        self.task_chain = task_chain_from_dict(task_chain_name='report', task_chain=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the data attribute of each task in the task chain is None
        self.assertGreater(self.task_chain[-1].data.get('total_bytes_pruned'), 0)
        [self.assertIsNone(task.data) for task in self.task_chain[0:-1]]

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
        self.task_chain = task_chain_from_dict(task_chain_name='report', task_chain=self.task_configuration)

        # run the task chain
        self.task_chain.run()

        # Check that the data attribute of each task in the task before the PruneTask is None
        self.assertGreater(self.task_chain[3].data.get('total_bytes_pruned'), 0)
        [self.assertIsNone(task.data) for task in self.task_chain[0:3]]
        self.assertEqual(self.task_chain[4].data, [{'dummy': 'data'}])
        self.assertEqual(self.task_chain[4].meta, {'info': 'this is dummy metadata'})

        # Check that the task chain did not result in error
        self.assertIsNone(self.task_chain.result.get('error'))
        self.assertEqual(self.task_chain.status, TaskStatusCodes.complete)


# TODO: Add tests for the TemplateTask
# TODO: Add tests for the WaitTask

if __name__ == '__main__':
    unittest.main()

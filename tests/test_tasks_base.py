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
        task_codes = TaskStatusCodes.__members__
        [self.assertTrue(code == TaskStatusCodes[code].value) for code in task_codes]


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
                        'wait': {
                            'name': 'wait_task',
                            'description': 'This is a wait task which tests a False when condition',
                            'when_after_seconds': 1,
                            'when': 'skip_var != \'run me\''
                        }
                    },
                    {
                        'wait': {
                            'name': 'wait_task',
                            'description': 'This is a wait task which tests a True when condition',
                            'when_after_seconds': 1,
                            'when': 'skip_var == \'run me\''
                        }
                    }
                ]
            }

        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.base_task_chain = task_chain_from_dict(task_chain_registered_class_name='report', task_chain=self.task_configuration)

    def test_instantiate(self):
        # Test the instantiate method
        self.base_task_chain.variables = {'skip_var': 'run me'}

        self.base_task_chain.run()
        self.assertIsNone(self.base_task_chain.result.get('error'))
        self.assertIsInstance(self.base_task_chain[0], DummyTask)
        self.assertIsInstance(self.base_task_chain[1], WaitTask)
        self.assertEqual(self.base_task_chain[0].status, TaskStatusCodes.complete)
        self.assertEqual(self.base_task_chain[1].status, TaskStatusCodes.skipped)
        self.assertEqual(self.base_task_chain[2].status, TaskStatusCodes.complete)
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.complete)


class TestBaseTask(BaseTestCase):
    def setUp(self):
        from CloudHarvestCorePluginManager.registry import Registry
        task = Registry.find_definition(class_name='wait', is_subclass_of=BaseTask)[0]
        self.base_task = task(name='test', description='test task', when_after_seconds=10)

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

    def test_retry(self):
        # Test the retry method
        from ..CloudHarvestCoreTasks.base import BaseTaskChain
        task_chain = BaseTaskChain(template={
            'name': 'test_chain',
            'description': 'This is a task_chain.',
            'tasks': [
                {
                    'error': {
                        'name': 'error_task 0',
                        'description': 'Testing max_attempts and delay',
                        'retry': {
                            'max_attempts': 3,
                            'delay': .001
                        }
                    }
                },
                {
                    'error': {
                        'name': 'error_task 1',
                        'description': 'Testing when_error_like (positive)',
                        'retry': {
                            'max_attempts': 3,
                            'delay_seconds': .001,
                            'when_error_like': 'This is an error task'
                        }
                    }
                },
                {
                    'error': {
                        'name': 'error_task 2',
                        'description': 'Testing when_error_like (negative)',
                        'retry': {
                            'max_attempts': 3,
                            'delay_seconds': .001,
                            'when_error_like': 'derp'
                        }
                    }
                },
                {
                    'error': {
                        'name': 'error_task 3',
                        'description': 'Testing when_error_not_like (positive)',
                        'retry': {
                            'max_attempts': 3,
                            'delay_seconds': .001,
                            'when_error_not_like': 'derp'
                        }
                    }
                },
                {
                    'error': {
                        'name': 'error_task 4',
                        'description': 'Testing when_error_not_like (negative)',
                        'retry': {
                            'max_attempts': 3,
                            'delay_seconds': .001,
                            'when_error_not_like': 'This is an error task'
                        }
                    }
                }
            ]
        })

        task_chain.run()

        # Testing max_attempts and delay
        self.assertEqual(task_chain[0].status, TaskStatusCodes.error)
        self.assertEqual(task_chain[0].attempts, 3)

        # Testing when_error_like (positive)
        self.assertEqual(task_chain[1].status, TaskStatusCodes.error)
        self.assertEqual(task_chain[1].attempts, 3)

        # Testing when_error_like (negative)
        self.assertEqual(task_chain[2].status, TaskStatusCodes.error)
        self.assertEqual(task_chain[2].attempts, 1)

        # Testing when_error_not_like (positive)
        self.assertEqual(task_chain[3].status, TaskStatusCodes.error)
        self.assertEqual(task_chain[3].attempts, 3)

        # Testing when_error_not_like (negative)
        self.assertEqual(task_chain[4].status, TaskStatusCodes.error)
        self.assertEqual(task_chain[4].attempts, 1)

    def test_on_skipped(self):
        self.base_task.on_skipped()
        self.assertEqual(self.base_task.status, TaskStatusCodes.skipped)

    def test_terminate(self):
        # Test the terminate method
        self.base_task.terminate()
        self.assertEqual(self.base_task.status, TaskStatusCodes.terminating)


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
                        'wait': {
                            'name': 'wait_task',
                            'description': 'This is a wait task',
                            'when_after_seconds': 1
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


class TestBaseTaskChainOnDirective(BaseTestCase):
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
                            'description': 'This is a dummy task which should always succeed.'
                        }
                    },
                    {
                        'dummy': {
                            'name': 'dummy_task',
                            'description': 'This is a dummy task which should succeed then run the on_complete directive.',
                            'on': {
                                'complete': [
                                    {
                                        'dummy': {
                                            'name': 'dummy_task',
                                                'description': 'This is a dummy task which runs when the previous task completes.'
                                            }
                                    }
                                ]
                            }
                        }
                    },
                    {
                        'error': {
                            'name': 'error_task',
                            'description': 'This task should always end in the error state and is used to test the on_error directive.',
                            'on':
                                {
                                    'error': [
                                            {
                                            'dummy': {
                                                'name': 'dummy_task',
                                                'description': 'This is a dummy task which runs when the previous task errors.'
                                            }
                                        }
                                    ]
                                }
                        }
                    },
                    {
                        'dummy': {
                            'name': 'dummy_skipped_task',
                            'description': 'This is a dummy task which runs when the previous task is skipped.',
                            'when': 'undefined_var = \'some value we will not supplys\'',
                            'on': {
                                'skipped': [
                                    {
                                        'dummy': {
                                            'name': 'dummy_task',
                                            'description': 'This is a dummy task which runs when the previous task is skipped.'
                                        }
                                    }
                                ]
                            }
                        }
                    }
                ]
        }

        from ..CloudHarvestCoreTasks.factories import task_chain_from_dict
        self.base_task_chain = task_chain_from_dict(task_chain_registered_class_name='chain', task_chain=self.task_configuration)

    def test_on_directives(self):
        self.base_task_chain.run()

        # This is the control task which always succeeds
        self.assertEqual(self.base_task_chain[0].status, TaskStatusCodes.complete)

        # This task will succeed then run the on_complete directive
        self.assertEqual(self.base_task_chain[1].status, TaskStatusCodes.complete)

        # This next task was created by the previous task's on_complete directive
        self.assertEqual(self.base_task_chain[2].status, TaskStatusCodes.complete)

        # This task will always end in the error state
        self.assertEqual(self.base_task_chain[3].status, TaskStatusCodes.error)

        # This next task was created by the previous task's on_error directive
        self.assertEqual(self.base_task_chain[4].status, TaskStatusCodes.complete)

        # This task will always be skipped
        self.assertEqual(self.base_task_chain[5].status, TaskStatusCodes.skipped)

        # This next task was created by the previous task's on_skipped directive
        self.assertEqual(self.base_task_chain[6].status, TaskStatusCodes.complete)

        # Verify that the task chain completed successfully
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.complete)


class TestBaseTaskPool(BaseTestCase):
    def setUp(self):
        template = {
            'name': 'test_chain',
            'description': 'This is a TaskChain used to test the BaseTaskPool.',
            'tasks': [
                {
                    'dummy': {
                        'name': 'Control Task 1',
                        'blocking': True,
                        'description': 'This is a standard Task which should always succeed.',
                    }
                },
                {
                    'wait': {
                        'name': 'wait task 1',
                        'blocking': False,
                        'description': 'This is a wait task',
                        'when_after_seconds': 5
                    }
                },
                {
                    'wait': {
                        'name': 'wait task 2',
                        'blocking': False,
                        'description': 'This is a wait task',
                        'when_after_seconds': 5
                    }
                },
                {
                    'wait': {
                        'name': 'wait task 3',
                        'blocking': False,
                        'description': 'This is a wait task',
                        'when_after_seconds': 5,
                        'on': {
                            'complete': [
                                {
                                    'dummy': {
                                        'name': 'Async Child Dummy Task',
                                        'description': 'This is a dummy task which should always succeed AFTER its parent task completes.',
                                    }
                                }
                            ]
                        }

                    }
                },
                {
                    'dummy': {
                        'name': 'Control Task 2',
                        'blocking': False,
                        'description': 'This is a dummy task which should always succeed BEFORE any of the wait tasks complete.',
                    }
                }
            ]
        }
        from ..CloudHarvestCoreTasks.base import BaseTaskChain
        self.base_task_chain = BaseTaskChain(template=template)

    def test_pooling(self):
        # Send the task chain to the background so we can monitor state changes
        from threading import Thread
        test_thread = Thread(target=self.base_task_chain.run)
        test_thread.start()

        # Make sure all tasks have been instantiated
        from time import sleep
        while len(self.base_task_chain) < 5:
            sleep(.1)

        # Make sure all tasks have started or completed
        while not all([task.status in [TaskStatusCodes.complete, TaskStatusCodes.running] for task in self.base_task_chain]):
            sleep(.1)

        # Make sure the task chain is still running
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.running)

        # Make sure the control blocking task is complete
        self.assertEqual(self.base_task_chain.find_task_by_name('Control Task 1').status, TaskStatusCodes.complete)

        # Make sure the non-blocking tasks are still running
        self.assertEqual(self.base_task_chain.find_task_by_name('wait task 1').status, TaskStatusCodes.running)
        self.assertEqual(self.base_task_chain.find_task_by_name('wait task 2').status, TaskStatusCodes.running)
        self.assertEqual(self.base_task_chain.find_task_by_name('wait task 3').status, TaskStatusCodes.running)

        # Make sure the final control task is complete
        self.assertEqual(self.base_task_chain.find_task_by_name('Control Task 2').status, TaskStatusCodes.complete)

        # Wait until the task chain is complete
        while self.base_task_chain.status != TaskStatusCodes.complete:
            sleep(.5)

        # Verify that wait task 2's child on_complete task succeeded
        self.assertEqual(self.base_task_chain.find_task_by_name('Async Child Dummy Task').status, TaskStatusCodes.complete)

        # Assert that all tasks in the pool have completed
        self.assertEqual(self.base_task_chain.status, TaskStatusCodes.complete)
        [
            self.assertEqual(task.status, TaskStatusCodes.complete) for task in self.base_task_chain
        ]


if __name__ == '__main__':
    unittest.main()

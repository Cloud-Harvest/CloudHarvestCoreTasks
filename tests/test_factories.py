"""
This module contains tests the "tasks.factories" module.
"""

import unittest
from ..CloudHarvestCoreTasks.tasks import BaseTaskChain
from ..CloudHarvestCoreTasks.tasks.factories import replace_variable_path_with_value

class TestReplaceVariablePathWithValue(unittest.TestCase):
    def setUp(self):
        self.task_chain = BaseTaskChain(template={'name': 'TestBaseTaskChainReplaceVariablePathWithValue'})
        self.task_chain.variables['replace_test'] = {
            'test_str': 'successful test str replacement',
            'test_list': [
                'index 0',
                'index 1',
                'index 2'
            ],
            'test_nested_dict': {
                'key1': 'value1',
                'key2': 'value2'
            },
            'test_nested_list_dict': {
                'key1': 'value1',
                'key2': 'value2',
                'key3': [
                    'index 0',
                    'index 1',
                    'index 2'
                ],
                'key4': {
                    'nested_key1': 'nested_value1',
                    'nested_key2': 'nested_value2',
                    'nested_key3': [
                        'index 0'
                    ]
                }
            }
        }            
    
    def test_replace_variable_path_with_value(self):
        # Test no replacement
        self.assertEqual(replace_variable_path_with_value(original_string='Unchanged text',
                                                          task_chain=self.task_chain),'Unchanged text')

        # Simple string replacement
        self.assertEqual(replace_variable_path_with_value(original_string='My string var.replace_test.test_str',
                                                          task_chain=self.task_chain),
            'My string successful test str replacement')

        # Multi-string replacement
        self.assertEqual(replace_variable_path_with_value(original_string='My string var.replace_test.test_str var.replace_test.test_nested_list_dict.key3[2]',
                                                          task_chain=self.task_chain),
                         'My string successful test str replacement index 2')

        # Nested string replacement
        self.assertEqual(replace_variable_path_with_value(original_string='My string var.replace_test.test_nested_list_dict.key4.nested_key3[0]',
                                                          task_chain=self.task_chain),
                         'My string index 0')

        # Whole object replacement
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict',
                                                          task_chain=self.task_chain),
                         self.task_chain.variables['replace_test']['test_nested_dict'])

        # Reference to unassigned variable
        self.assertEqual(replace_variable_path_with_value(original_string='var.unassigned_variable.key1',
                                                          task_chain=self.task_chain),
                         'var.unassigned_variable.key1')

        # Invalid path will raise KeyError
        self.assertRaises(KeyError,
                          replace_variable_path_with_value,
                          **{
                              'original_string': 'var.replace_test.test_nested_dict.key3',
                              'task_chain': self.task_chain
                          })

        # Testing item iteration
        items = [
            {
                'name': 'John',
                'age': 25
            },
            {
                'name': 'Jane',
                'age': 47
            }
        ]

        for item in items:
            self.assertEqual(replace_variable_path_with_value(original_string='My name is item.name',
                                                              item=item),
                f"My name is {item['name']}")

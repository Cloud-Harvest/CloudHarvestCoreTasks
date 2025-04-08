"""
This module contains tests the "tasks.factories" module.
"""

import unittest
from chains.base import BaseTaskChain
from CloudHarvestCoreTasks.factories import replace_variable_path_with_value

class TestReplaceVariablePathWithValue(unittest.TestCase):
    def setUp(self):
        from CloudHarvestCoreTasks.environment import Environment
        self.env = Environment
        self.env.add('my_environment_variable', 'My env string value')

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

        # Environment variable replacement
        self.assertEqual(replace_variable_path_with_value(original_string='env.my_environment_variable',
                                                          task_chain=self.task_chain),
                            'My env string value')

        # Variable and Environment variable replacement
        self.assertEqual(replace_variable_path_with_value(original_string='env.my_environment_variable var.replace_test.test_str',
                                                          task_chain=self.task_chain),
                         'My env string value successful test str replacement')

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

    def test_method_and_property_paths(self):
        # Test var.replace_test.test_nested_dict.keys()
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict.keys',
                                                          task_chain=self.task_chain),
                         ['key1', 'key2'])

        # Test var.replace_test.test_list[0].value
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_list[0].value',
                                                          task_chain=self.task_chain),
                         'index 0')

        # Test var.replace_test.test_nested_dict.values()
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict.values',
                                                          task_chain=self.task_chain),
                         ['value1', 'value2'])


        # Test nested special values
        # Test var.replace_test.test_list[0].keys[0] which should return the first key of the first item in the list
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict.keys[0]',
                                                          task_chain=self.task_chain),
                         'key1')

        # Test nested special values
        # Test var.replace_test.test_list[0].keys[0] which should return the first key of the first item in the list
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict.keys[0]',
                                                          task_chain=self.task_chain),
                         'key1')

        # Test var.replace_test.test_list[0].keys[0].upper() which should return the first key of the first item in the list
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict.keys[0].upper',
                                                          task_chain=self.task_chain),
                         'KEY1')

        # Test var.replace_test.test_list[0].keys[0] which should return the first key of the first item in the list
        # TODO: maybe we shouldn't allow methods like __len__ to be called...
        self.assertEqual(replace_variable_path_with_value(original_string='var.replace_test.test_nested_dict.keys[0].upper.__len__',
                                                          task_chain=self.task_chain),
                         4)

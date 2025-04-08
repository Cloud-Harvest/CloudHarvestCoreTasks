import unittest

class TestEnvironment(unittest.TestCase):
    def setUp(self):
        from CloudHarvestCoreTasks.environment import Environment

        self.env = Environment
        self.env.add('TEST_VAR', 'test_value')

        # Write a test YAML file for the LOAD test
        self.yaml_file = 'test_env.yaml'
        with open(self.yaml_file, 'w') as f:
            from yaml import dump
            dump({'FILE_TEST_VAR': 'file_test_value'}, f)

        # Write a test JSON file for the LOAD test
        self.json_file = 'test_env.json'
        with open(self.json_file, 'w') as f:
            from json import dump
            dump({'FILE_TEST_VAR': 'file_test_value'}, f, default=str)

    def tearDown(self):
        from os import remove
        from os.path import exists

        # Remove test files if they exist
        for file in (self.yaml_file, self.json_file):
            if exists(file):
                remove(file)

        # Purge the environment variables
        self.env.purge()

    def test_add_variable(self):
        self.env.add('NEW_VAR', 'new_value')
        self.assertEqual(self.env.get('NEW_VAR'), 'new_value')

        # test override=False
        self.env.add('NEW_VAR', 'new_value_2')
        self.assertEqual(self.env.get('NEW_VAR'), 'new_value')

        # test override=True
        self.env.add('NEW_VAR', 'new_value_2', overwrite=True)
        self.assertEqual(self.env.get('NEW_VAR'), 'new_value_2')

    def test_overwrite_variable(self):
        self.env.add('TEST_VAR', 'new_value', overwrite=True)
        self.assertEqual(self.env.get('TEST_VAR'), 'new_value')

    def test_get_variable(self):
        self.assertEqual(self.env.get('TEST_VAR'), 'test_value')
        self.assertIsNone(self.env.get('NON_EXISTENT_VAR'))  # Default is None

    def test_purge_variables(self):
        self.env.purge()
        self.assertIsNone(self.env.get('TEST_VAR'))

    def test_load_yaml_file(self):
        self.env.load('test_env.yaml')
        self.assertEqual(self.env.get('FILE_TEST_VAR'), 'file_test_value')

    def test_load_json_file(self):
        # Assuming you have a valid JSON file for testing
        self.env.load('test_env.json')
        self.assertEqual(self.env.get('FILE_TEST_VAR'), 'file_test_value')

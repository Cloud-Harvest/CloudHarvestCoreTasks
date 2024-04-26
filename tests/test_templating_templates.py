import unittest
from harvest.templating.functions import template_object


class TestTemplateObject(unittest.TestCase):
    def test_template_object(self):
        # Test with a string template
        template = '{"greeting": "Hello, {{ name }}!"}'
        variables = {'name': 'World'}
        expected_output = {"greeting": "Hello, World!"}
        self.assertEqual(template_object(template, variables), expected_output)

        # Test with a non-string template
        template = {'greeting': 'Hello, {{ name }}!'}
        variables = {'name': 'World'}
        expected_output = {"greeting": "Hello, World!"}
        self.assertEqual(template_object(template, variables), expected_output)

        # Test with no variables
        template = '{"greeting": "Hello, {{ name }}!"}'
        expected_output = {"greeting": "Hello, !"}
        self.assertEqual(template_object(template), expected_output)


if __name__ == '__main__':
    unittest.main()

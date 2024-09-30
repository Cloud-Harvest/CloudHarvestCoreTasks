import unittest
from tasks.templating import template_object


class TestTemplateObject(unittest.TestCase):
    def test_template_object(self):
        # Test with a string template
        template = '{"greeting": "Hello, {{ name }}!"}'
        variables = {'name': 'World'}
        expected_output = {"greeting": "Hello, World!"}
        result = template_object(template, variables)
        self.assertEqual(result, expected_output)

        # Test with a non-string template
        template = {'greeting': 'Hello, {{ name }}!'}
        variables = {'name': 'World'}
        expected_output = {"greeting": "Hello, World!"}
        result = template_object(template, variables)
        self.assertEqual(result, expected_output)

        # Test with no variables
        template = '{"greeting": "Hello, {{ name }}!"}'
        expected_output = {"greeting": "Hello, !"}
        result = template_object(template)
        self.assertEqual(result, expected_output)

        # Assert that a template with a condition is rendered correctly TRUE
        template = {'result': "{{ name == 'World' }}"}
        variables = {'name': 'World'}
        result = template_object(template, variables)
        self.assertEqual(result['result'], 'True')

        # Assert that a template with a condition is rendered correctly FALSE
        template = {'result': "{{ name == 'Something else' }}"}
        variables = {'name': 'World'}
        result = template_object(template, variables)
        self.assertEqual(result['result'], 'False')

if __name__ == '__main__':
    unittest.main()

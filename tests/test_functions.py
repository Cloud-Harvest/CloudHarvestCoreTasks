import unittest
import CloudHarvestCoreTasks.functions as functions


class TestFunctions(unittest.TestCase):
    """
    Test case for the functions in functions.py
    """

    def test_cast(self):
        """
        Test the cast function with different types of inputs
        """

        # Test casting an integer to a string
        self.assertEqual(functions.cast(1, 'str'), '1')

        # Test casting a float to an integer
        self.assertEqual(functions.cast(1.3, 'int'), 1)

        # Test casting a string to an integer
        self.assertEqual(functions.cast('1', 'int'), 1)

        # Test casting an integer to a float
        self.assertEqual(functions.cast(1, 'float'), 1.0)

        # Test casting a string to a boolean
        self.assertEqual(functions.cast('False', 'bool'), False)
        self.assertEqual(functions.cast('No', 'bool'), False)
        self.assertEqual(functions.cast('Yes', 'bool'), True)

        # Test casting an invalid string to an integer
        self.assertIsNone(functions.cast('invalid', 'int'))

    def test_delimiter_list_to_string(self):
        """
        Test the delimiter_list_to_string function with different types of inputs
        """

        # Test joining a list of strings with a comma
        self.assertEqual(functions.delimiter_list_to_string(['a', 'b', 'c'], ','), 'a,b,c')

        # Test joining a list of strings with a hyphen
        self.assertEqual(functions.delimiter_list_to_string(['1', '2', '3'], '-'), '1-2-3')

    def test_key_value_list_to_dict(self):
        """
        Test the key_value_list_to_dict function with different types of inputs
        """

        # Test converting a list of dictionaries to a dictionary
        self.assertEqual(functions.key_value_list_to_dict([{'Key': 'Name', 'Value': 'MyName'}]), {'Name': 'MyName'})

        # Test converting a list of dictionaries with multiple key-value pairs to a dictionary
        self.assertEqual(
            functions.key_value_list_to_dict([{'Key': 'Name', 'Value': 'MyName'}, {'Key': 'Age', 'Value': '30'}]), {'Name': 'MyName', 'Age': '30'})

    def test_is_number(self):
        """
        Test the is_number function with different types of inputs
        """

        # Test checking if a string is a number
        self.assertTrue(functions.is_number('123'))

        # Test checking if a string is not a number
        self.assertFalse(functions.is_number('abc'))


if __name__ == '__main__':
    unittest.main()

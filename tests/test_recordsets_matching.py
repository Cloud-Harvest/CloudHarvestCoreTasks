import unittest
from collections import OrderedDict
from ..CloudHarvestCoreTasks.data_model import matching


class TestMatching(unittest.TestCase):
    """
    Test case for the classes in matching.py
    """

    def test_HarvestMatch(self):
        """
        Test the HarvestMatch class with different types of inputs
        """
        # Test creating a HarvestMatch object and calling the match method
        record = OrderedDict([('key1', '1'), ('key2', '2')])

        # Test '==' operator
        syntax = 'key1==1'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1==1')
        self.assertTrue(match.is_match)

        # Test '>=' operator
        syntax = 'key2>=2'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '2>=2')
        self.assertTrue(match.is_match)

        # Test '<=' operator
        syntax = 'key2<=2'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '2<=2')
        self.assertTrue(match.is_match)

        # Test '!=' operator
        syntax = 'key1!=2'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1!=2')
        self.assertTrue(match.is_match)

        # Test '>' operator
        syntax = 'key2>1'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '2>1')
        self.assertTrue(match.is_match)

        # Test '<' operator
        syntax = 'key1<2'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1<2')
        self.assertTrue(match.is_match)

        # Test '=' operator
        syntax = 'key1=1'
        match = matching.HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1=1')
        self.assertTrue(match.is_match)

    def test_HarvestMatchSet(self):
        """
        Test the HarvestMatchSet class with different types of inputs
        """
        # Test creating a HarvestMatchSet object
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2=value2']
        match_set = matching.HarvestMatchSet(matches=matches, record=record)
        self.assertEqual(len(match_set.matches), 2)

        # Test creating a HarvestMatchSet object with successful matches
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1']
        match_set = matching.HarvestMatchSet(matches=matches, record=record)
        self.assertTrue(len(match_set.matches[0].match()), 0)

        # Test creating a HarvestMatchSet object with no matches
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=DERP']
        match_set = matching.HarvestMatchSet(matches=matches, record=record)
        self.assertFalse(len(match_set.matches[0].match()), 0)


if __name__ == '__main__':
    unittest.main()

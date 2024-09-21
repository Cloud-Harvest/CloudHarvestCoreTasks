import unittest
from collections import OrderedDict
from ..CloudHarvestCoreTasks.data_model.matching import (
    HarvestMatch,
    HarvestMatchSet,
    build_mongo_matching_syntax,
    build_sql_matching_syntax
)

class TestHarvestMatch(unittest.TestCase):
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
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1==1')
        self.assertTrue(match.is_match)

        # Test '>=' operator
        syntax = 'key2>=2'
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '2>=2')
        self.assertTrue(match.is_match)

        # Test '<=' operator
        syntax = 'key2<=2'
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '2<=2')
        self.assertTrue(match.is_match)

        # Test '!=' operator
        syntax = 'key1!=2'
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1!=2')
        self.assertTrue(match.is_match)

        # Test '>' operator
        syntax = 'key2>1'
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '2>1')
        self.assertTrue(match.is_match)

        # Test '<' operator
        syntax = 'key1<2'
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1<2')
        self.assertTrue(match.is_match)

        # Test '=' operator
        syntax = 'key1=1'
        match = HarvestMatch(record=record, syntax=syntax)
        self.assertTrue(match.match())
        self.assertEqual(match.final_match_operation, '1=1')
        self.assertTrue(match.is_match)

    def test_as_mongo_filter(self):

        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])

        # Test '=' operator
        match = HarvestMatch(syntax='key1=value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$regex': 'value1', '$options': 'i'}})

        # Test '==' operator
        match = HarvestMatch(syntax='key1==value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': 'value1'})

        # Test '!=' operator
        match = HarvestMatch(syntax='key1!=value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$ne': 'value1'}})

        # Test '<=' operator
        match = HarvestMatch(syntax='key1<=value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$lte': 'value1'}})

        # Test '>=' operator
        match = HarvestMatch(syntax='key1>=value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$gte': 'value1'}})

        # Test '<' operator
        match = HarvestMatch(syntax='key1<value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$lt': 'value1'}})

        # Test '>' operator
        match = HarvestMatch(syntax='key1>value1', record=record)
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$gt': 'value1'}})

    def test_as_sql_filter(self):
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])

        # Test '=' operator
        match = HarvestMatch(syntax='key1=value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 ILIKE '%value1%'")

        # Test '==' operator
        match = HarvestMatch(syntax='key1==value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 = 'value1'")

        # Test '!=' operator
        match = HarvestMatch(syntax='key1!=value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 != 'value1'")

        # Test '<=' operator
        match = HarvestMatch(syntax='key1<=value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 <= 'value1'")

        # Test '>=' operator
        match = HarvestMatch(syntax='key1>=value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 >= 'value1'")

        # Test '<' operator
        match = HarvestMatch(syntax='key1<value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 < 'value1'")

        # Test '>' operator
        match = HarvestMatch(syntax='key1>value1', record=record)
        self.assertEqual(match.as_sql_filter(), "key1 > 'value1'")

    def test_HarvestMatchSet(self):
        """
        Test the HarvestMatchSet class with different types of inputs
        """
        # Test creating a HarvestMatchSet object
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2=value2']
        match_set = HarvestMatchSet(matches=matches, record=record)
        self.assertEqual(len(match_set.matches), 2)

        # Test creating a HarvestMatchSet object with successful matches
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1']
        match_set = HarvestMatchSet(matches=matches, record=record)
        self.assertTrue(len(match_set.matches[0].match()), 0)

        # Test creating a HarvestMatchSet object with no matches
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=DERP']
        match_set = HarvestMatchSet(matches=matches, record=record)
        self.assertFalse(len(match_set.matches[0].match()), 0)

class TestHarvestMatchSet(unittest.TestCase):
    def test_as_mongo_filter(self):
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2!=value2']

        # Create a HarvestMatchSet instance
        match_set = HarvestMatchSet(matches=matches, record=record)

        # Test AND logic
        expected_filter = {
            '$and': [
                {'key1': {'$regex': 'value1', '$options': 'i'}},
                {'key2': {'$ne': 'value2'}}
            ]
        }
        self.assertEqual(match_set.as_mongo_filter(), expected_filter)

        # Test single match condition
        single_match_set = HarvestMatchSet(matches=['key1=value1'], record=record)
        self.assertEqual(single_match_set.as_mongo_filter(), {'key1': {'$regex': 'value1', '$options': 'i'}})

    def test_as_sql_filter(self):
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2!=value2']

        # Create a HarvestMatchSet instance
        match_set = HarvestMatchSet(matches=matches, record=record)

        # Test AND logic
        expected_filter = "key1 ILIKE '%value1%' AND key2 != 'value2'"
        self.assertEqual(match_set.as_sql_filter(), expected_filter)

        # Test single match condition
        single_match_set = HarvestMatchSet(matches=['key1=value1'], record=record)
        self.assertEqual(single_match_set.as_sql_filter(), "key1 ILIKE '%value1%'")

class TestBuildMatchingSyntax(unittest.TestCase):
    def test_build_mongo_matching_syntax(self):
        matches = [['key1=value1'], ['key2!=value2']]
        expected_filter = {
            '$or': [
                {'key1': {'$regex': 'value1', '$options': 'i'}},
                {'key2': {'$ne': 'value2'}}
            ]
        }
        self.assertEqual(build_mongo_matching_syntax(matches), expected_filter)

        matches = [['key1=value1', 'key2!=value2']]
        expected_filter_and = {
            '$and': [
                {'key1': {'$regex': 'value1', '$options': 'i'}},
                {'key2': {'$ne': 'value2'}}
            ]
        }
        self.assertEqual(build_mongo_matching_syntax(matches), expected_filter_and)

        matches = [['key1=value1']]
        expected_filter_single = {'key1': {'$regex': 'value1', '$options': 'i'}}
        self.assertEqual(build_mongo_matching_syntax(matches), expected_filter_single)

    def test_build_sql_matching_syntax(self):
        matches = [['key1=value1'], ['key2!=value2']]
        expected_filter = "key1 ILIKE '%value1%' OR key2 != 'value2'"
        self.assertEqual(build_sql_matching_syntax(matches), expected_filter)

        matches = [['key1=value1', 'key2!=value2']]
        expected_filter_and = "key1 ILIKE '%value1%' AND key2 != 'value2'"
        self.assertEqual(build_sql_matching_syntax(matches), expected_filter_and)

        matches = [['key1=value1']]
        expected_filter_single = "key1 ILIKE '%value1%'"
        self.assertEqual(build_sql_matching_syntax(matches), expected_filter_single)


if __name__ == '__main__':
    unittest.main()

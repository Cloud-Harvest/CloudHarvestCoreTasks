import unittest
from collections import OrderedDict
from ..CloudHarvestCoreTasks.data_model.matching import (
    HarvestMatch,
    HarvestMatchSet
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
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '1==1')

        # Test '>=' operator
        syntax = 'key2>=2'
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '2>=2')
        
        # Test '<=' operator
        syntax = 'key2<=2'
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '2<=2')
        
        # Test '!=' operator
        syntax = 'key1!=2'
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '1!=2')
        
        # Test '>' operator
        syntax = 'key2>1'
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '2>1')
        
        # Test '<' operator
        syntax = 'key1<2'
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '1<2')
        
        # Test '=' operator
        syntax = 'key1=1'
        match = HarvestMatch(syntax=syntax)
        self.assertTrue(match.match(record=record))
        self.assertEqual(match.final_match_operation, '1=1')
        

    def test_as_mongo_filter(self):
        # Test '=' operator
        match = HarvestMatch(syntax='key1=value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$regex': 'value1', '$options': 'i'}})

        # Test '==' operator
        match = HarvestMatch(syntax='key1==value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': 'value1'})

        # Test '!=' operator
        match = HarvestMatch(syntax='key1!=value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$ne': 'value1'}})

        # Test '<=' operator
        match = HarvestMatch(syntax='key1<=value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$lte': 'value1'}})

        # Test '>=' operator
        match = HarvestMatch(syntax='key1>=value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$gte': 'value1'}})

        # Test '<' operator
        match = HarvestMatch(syntax='key1<value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$lt': 'value1'}})

        # Test '>' operator
        match = HarvestMatch(syntax='key1>value1')
        self.assertEqual(match.as_mongo_filter(), {'key1': {'$gt': 'value1'}})

    def test_as_sql_filter(self):
        # Test '=' operator
        match = HarvestMatch(syntax='key1=value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 ILIKE '%value1%'")

        # Test '==' operator
        match = HarvestMatch(syntax='key1==value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 = 'value1'")

        # Test '!=' operator
        match = HarvestMatch(syntax='key1!=value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 != 'value1'")

        # Test '<=' operator
        match = HarvestMatch(syntax='key1<=value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 <= 'value1'")

        # Test '>=' operator
        match = HarvestMatch(syntax='key1>=value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 >= 'value1'")

        # Test '<' operator
        match = HarvestMatch(syntax='key1<value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 < 'value1'")

        # Test '>' operator
        match = HarvestMatch(syntax='key1>value1')
        self.assertEqual(sql_resolver(filter_result=match.as_sql_filter()), "key1 > 'value1'")

    def test_HarvestMatchSet(self):
        """
        Test the HarvestMatchSet class with different types of inputs
        """
        # Test creating a HarvestMatchSet object
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2=value2']
        match_set = HarvestMatchSet(matches=matches)
        self.assertEqual(len(match_set.matches), 2)

        # Test creating a HarvestMatchSet object with successful matches
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1']
        match_set = HarvestMatchSet(matches=matches)
        self.assertTrue(len(match_set.matches[0].match(record=record)), 0)

        # Test creating a HarvestMatchSet object with no matches
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=DERP']
        match_set = HarvestMatchSet(matches=matches)
        self.assertFalse(len(match_set.matches[0].match(record=record)), 0)

class TestHarvestMatchSet(unittest.TestCase):
    def test_as_mongo_filter(self):
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2!=value2']

        # Create a HarvestMatchSet instance
        match_set = HarvestMatchSet(matches=matches)

        # Test AND logic
        expected_filter = {
            '$and': [
                {'key1': {'$regex': 'value1', '$options': 'i'}},
                {'key2': {'$ne': 'value2'}}
            ]
        }
        self.assertEqual(match_set.as_mongo_filter(), expected_filter)

        # Test single match condition
        single_match_set = HarvestMatchSet(matches=['key1=value1'])
        self.assertEqual(single_match_set.as_mongo_filter(), {'key1': {'$regex': 'value1', '$options': 'i'}})

    def test_as_sql_filter(self):
        record = OrderedDict([('key1', 'value1'), ('key2', 'value2')])
        matches = ['key1=value1', 'key2!=value2']

        # Create a HarvestMatchSet instance
        match_set = HarvestMatchSet(matches=matches)

        # Test AND logic
        expected_filter = "key1 ILIKE '%value1%' AND key2 != 'value2'"
        self.assertEqual(sql_resolver(filter_result=match_set.as_sql_filter()), expected_filter)

        # Test single match condition
        single_match_set = HarvestMatchSet(matches=['key1=value1'])
        self.assertEqual(sql_resolver(single_match_set.as_sql_filter()), "key1 ILIKE '%value1%'")


def sql_resolver(filter_result: tuple):
    """
    Helper function to resolve the SQL filter result
    """
    from copy import copy
    sql_string, sql_params = filter_result

    result = copy(sql_string)
    for key, value in sql_params.items():
        result = result.replace(f'%({key})s', value)

    return result

if __name__ == '__main__':
    unittest.main()

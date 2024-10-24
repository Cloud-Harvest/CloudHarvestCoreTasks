import unittest
from ..CloudHarvestCoreTasks.data_model import HarvestRecord


class TestHarvestRecord(unittest.TestCase):
    """
    Test case for the HarvestRecord class in record.py
    """

    def setUp(self):
        """
        Set up a HarvestRecord object for use in tests
        """
        self.record = HarvestRecord(key1='value1', key2='value2')

    def test_add_freshness(self):
        """
        Test the add_freshness method
        """
        from datetime import datetime, timezone

        self.record['Active'] = True
        self.record['LastSeen'] = str(datetime(2020, 1, 1, tzinfo=timezone.utc))

        # test a stale record state
        self.record.add_freshness()
        self.assertEqual(self.record['f'], 'S')

        self.record['LastSeen'] = str(datetime.now(tz=timezone.utc))

        # test a fresh record state
        self.record.add_freshness()
        self.assertEqual(self.record['f'], 'F')

        # test an inactive record state
        self.record['Active'] = False
        self.record.add_freshness()
        self.assertEqual(self.record['f'], 'I')

        # remove the fields used for this test
        self.record.pop('Active')
        self.record.pop('LastSeen')

    def test_assign_elements_at_index_to_key(self):
        """
        Test the assign_elements_at_index_to_key method
        """
        self.record['list_example'] = ['key1', 'key2', 'key3', 'key4']
        self.record.assign_elements_at_index_to_key('list_example', 'new_key', 0, 2, ',')
        self.assertEqual(self.record['new_key'], 'key1,key2')

    def test_cast(self):
        """
        Test the cast method. cast() is a call to functions.cast().
        """
        self.record.cast('key1', 'int')
        self.assertEqual(self.record['key1'], None)

        self.record['key4'] = '4'

        self.record.cast('key4', 'int')
        self.assertEqual(self.record['key4'], 4)

        self.record.pop('key4')

    def test_copy_key(self):
        """
        Test the copy_key method
        """
        self.record.copy_key('key1', 'key3')
        self.assertEqual(self.record['key3'], 'value1')

    def test_dict_from_json_string(self):
        """
        Test the dict_from_json_string method
        """

        # store the test record for later restoration
        original_record = self.record.copy()

        # Test 'key' operation
        self.record['json'] = '{"key1": "value1", "key2": "value2"}'
        self.record.dict_from_json_string('json', 'key', 'new_key')
        self.assertEqual(self.record['new_key'], {"key1": "value1", "key2": "value2"})

        # replace the original record
        self.record.update(original_record)

        # Test 'merge' operation
        self.record['json'] = '{"key3": "value3"}'
        self.record.dict_from_json_string('json', 'merge')
        self.assertEqual(self.record['key3'], "value3")

        # replace the original record
        self.record.update(original_record)

        # Test 'replace' operation
        self.record['json'] = '{"key4": "value4"}'
        self.record.dict_from_json_string('json', 'replace')
        self.assertEqual(self.record['json'], {"key4": "value4"})

        # replace the original record
        self.record.update(original_record)

    def test_first_not_null_value(self):
        """
        Test the first_not_null_value method
        """
        self.assertEqual(self.record.first_not_null_value('key1', 'key2'), 'value1')

    def test_flatten(self):
        """
        Test the flatten method
        """
        r = HarvestRecord()

        r['key1'] = {'key2': {'key3': 'value'}}
        r.flatten()

        self.assertEqual(r['key1.key2.key3'], 'value')
        self.assertTrue(r.is_flat)

    def test_is_matched_record(self):
        """
        Test the is_matched_record method
        """
        self.assertTrue(self.record.is_matched_record)

    def test_key_value_list_to_dict(self):
        """
        Test the key_value_list_to_dict method
        """
        self.record['KV'] = [{'Key': 'name', 'Value': 'value'}]
        self.record.key_value_list_to_dict('KV')
        self.assertEqual(self.record['KV'], {'name': 'value'})

    def test_remove_key(self):
        """
        Test the remove_key method
        """
        self.record.remove_key('key1')
        self.assertNotIn('key1', self.record)

    def test_rename_key(self):
        """
        Test the rename_key method
        """
        self.record.rename_key('key1', 'key3')
        self.assertNotIn('key1', self.record)
        self.assertIn('key3', self.record)

    def test_reset_matches(self):
        """
        Test the reset_matches method
        """
        self.record.reset_matches()
        self.assertEqual(self.record.matching_expressions, [])
        self.assertEqual(self.record.non_matching_expressions, [])

    def test_split_key(self):
        """
        Test the split_key method
        """
        self.record['key5'] = 'value1 value2 value3'
        self.record.split_key('key5', 'new_key')
        self.assertEqual(self.record['new_key'], ['value1', 'value2', 'value3'])

    def test_substring(self):
        """
        Test the substring method
        """
        self.record['key6'] = 'value1'
        self.record.substring('key6', 0, 5, target_key='key7')
        self.assertEqual(self.record['key7'], 'value')

        self.record.substring('key6', start=-1, target_key='key7')
        self.assertEqual(self.record['key7'], '1')

    def test_unflatten(self):
        """
        Test the unflatten method
        """

        r = HarvestRecord()

        r['key1.key2.key3'] = 'value'
        r.is_flat = True

        r.unflatten()

        self.assertEqual(r['key1']['key2']['key3'], 'value')
        self.assertFalse(r.is_flat)


if __name__ == '__main__':
    unittest.main()

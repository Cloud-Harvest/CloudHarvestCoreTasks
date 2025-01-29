import unittest
from ..CloudHarvestCoreTasks.dataset import DataSet, perform_maths_operation

class TestDataSet(unittest.TestCase):
    def setUp(self):

        self.dataset = DataSet()
        self.dataset.add_records([
            {
                'name': 'John Doe',
                'address': {
                    'street': '123 Main St',
                    'city': 'Anytown',
                    'state': 'CA',
                    'zip': '12345'
                },
                'dob': '1990-01-01',
                'email': 'john.doe@example.com',
                'phone': '555-1234',
                'tags': ['friend', 'colleague'],
                'notes': 'Met at conference',
                'age': 30,
                'active': True
            },
            {
                'name': 'Jane Smith',
                'address': {
                    'street': '456 Elm St',
                    'city': 'Othertown',
                    'state': 'TX',
                    'zip': '67890'
                },
                'dob': '1985-05-15',
                'email': 'jane.smith@example.com',
                'phone': '555-5678',
                'tags': ['family'],
                'notes': 'Cousin',
                'age': 35,
                'active': False
            }
        ])

        pass

    def test_add_keys(self):
        self.dataset.add_keys(['new_key'], default_value='default')
        for record in self.dataset:
            self.assertIn('new_key', record)
            self.assertEqual(record['new_key'], 'default')

    def test_cast_key(self):
        self.dataset.cast_key('age', 'str')
        for record in self.dataset:
            self.assertIsInstance(record['age'], str)

    def test_clean_keys(self):
        self.dataset.clean_keys(add_keys=['new_key'], exclude_keys=['phone'])
        for record in self.dataset:
            self.assertIn('new_key', record)
            self.assertNotIn('phone', record)

    def test_convert_list_of_dict_to_dict(self):
        for record in self.dataset:
            record['convert_tags'] = [{'Key': 'tag1', 'Value': 'value1'}, {'Key': 'tag2', 'Value': 'value2'}]

        self.dataset.convert_list_of_dict_to_dict('convert_tags', name_key='Key')
        for record in self.dataset:
            self.assertIn('convert_tags', record)
            self.assertIsInstance(record['convert_tags'], dict)
            self.assertEqual(record['convert_tags'], {'tag1': 'value1', 'tag2': 'value2'})

    def test_convert_list_to_string(self):
        self.dataset.convert_list_to_string('tags')
        for record in self.dataset:
            self.assertIsInstance(record['tags'], str)

    def test_convert_string_to_list(self):
        self.dataset.convert_string_to_list(source_key='tags', target_key='tags_list')

        for record in self.dataset:
            self.assertIsInstance(record['tags_list'], list)

    def test_copy_key(self):
        self.dataset.copy_key('name', 'full_name')
        for record in self.dataset:
            self.assertIn('full_name', record)
            self.assertEqual(record['full_name'], record['name'])

    def test_flatten_and_unflatten(self):
        self.dataset.flatten()
        for record in self.dataset:
            self.assertIn('address.street', record)
            self.assertIn('address.city', record)
        self.dataset.unflatten()
        for record in self.dataset:
            self.assertIn('address', record)
            self.assertIn('street', record['address'])
            self.assertIn('city', record['address'])

    def test_match_and_remove(self):
        self.dataset.match_and_remove(['name == "John Doe"'])
        for record in self.dataset:
            self.assertNotEqual(record['name'], 'John Doe')

    def test_maths_keys(self):
        self.dataset.maths_keys(source_keys=['age', 'age'],
                                target_key='age_plus_age',
                                operation='add',
                                missing_value=0,
                                default_value=None)

        for record in self.dataset:
            self.assertIn('age_plus_age', record)
            self.assertEqual(record['age_plus_age'], record['age'] * 2)

    def test_maths_records(self):
        self.dataset.maths_records(source_key='age',
                                   target_key='average_age',
                                   operation='average',
                                   missing_value=0,
                                   default_value=None)

        self.assertIn('average_age', self.dataset.maths_results)
        self.assertEqual(self.dataset.maths_results['average_age'], 32.5)

    def test_append_maths_results(self):
        self.dataset.maths_records(source_key='age',
                                   target_key='average_age',
                                   operation='average',
                                   missing_value=0,
                                   default_value=None)

        self.dataset.append_record_maths_results()
        self.assertEqual(self.dataset[-1]['_id'], 'Totals')

    def test_rename_keys(self):
        self.dataset.rename_keys([{'old': 'name', 'new': 'full_name'}])
        for record in self.dataset:
            self.assertIn('full_name', record)
            self.assertNotIn('name', record)

    def test_remove_duplicate_records(self):
        from copy import copy
        self.dataset.append(copy(self.dataset[0]))
        self.dataset.remove_duplicate_records()
        self.assertEqual(len(self.dataset), 2)

    def test_unwind_and_wind(self):
        self.dataset.unwind('tags')
        for record in self.dataset:
            self.assertIsInstance(record['tags'], str)

        self.dataset.wind('tags')

        for record in self.dataset:
            self.assertIsInstance(record['tags'], list)

class TestWalkableDict(unittest.TestCase):
    def setUp(self):

        self.dataset = DataSet([
            {
                'name': 'John Doe',
                'address': {
                    'street': '123 Main St',
                    'city': 'Anytown',
                    'state': 'CA',
                    'zip': '12345'
                },
                'dob': '1990-01-01',
                'email': 'john.doe@example.com',
                'phone': '555-1234',
                'tags': ['friend', 'colleague'],
                'notes': 'Met at conference',
                'age': 30,
                'active': True
            },
            {
                'name': 'Jane Smith',
                'address': {
                    'street': '456 Elm St',
                    'city': 'Othertown',
                    'state': 'TX',
                    'zip': '67890'
                },
                'dob': '1985-05-15',
                'email': 'jane.smith@example.com',
                'phone': '555-5678',
                'tags': ['family'],
                'notes': 'Cousin',
                'age': 35,
                'active': False
            }
        ])

    def test_walk(self):
        for record in self.dataset:
            self.assertEqual(record.walk('address.city'), record['address']['city'])

    def test_assign(self):
        for record in self.dataset:
            record.assign('address.city', 'New City')
            self.assertEqual(record['address']['city'], 'New City')

    def test_drop(self):
        for record in self.dataset:
            record.drop('address.city')
            self.assertNotIn('city', record['address'])


class TestPerformMathsOperation(unittest.TestCase):

    def test_add_operation(self):
        result = perform_maths_operation('add', [1, 2, 3, 4])
        self.assertEqual(result, 10)

    def test_subtract_operation(self):
        result = perform_maths_operation('subtract', [10, 2, 3])
        self.assertEqual(result, -15)

    def test_multiply_operation(self):
        result = perform_maths_operation('multiply', [2, 3, 4])
        self.assertEqual(result, 24)

    def test_divide_operation(self):
        result = perform_maths_operation('divide', [8, 2, 2])
        self.assertEqual(result, 2)

    def test_average_operation(self):
        result = perform_maths_operation('average', [2, 4, 6])
        self.assertEqual(result, 4)

    def test_minimum_operation(self):
        result = perform_maths_operation('minimum', [2, 4, 6])
        self.assertEqual(result, 2)

    def test_maximum_operation(self):
        result = perform_maths_operation('maximum', [2, 4, 6])
        self.assertEqual(result, 6)

    def test_invalid_operation(self):
        result = perform_maths_operation('invalid', [2, 4, 6])
        self.assertIsNone(result)

    def test_missing_values(self):
        result = perform_maths_operation('add', [])
        self.assertEqual(result, 0)

    def test_error_data(self):
        self.assertIsNone(perform_maths_operation('add', [2, 'a', 4]))


if __name__ == '__main__':
    unittest.main()
import unittest
from CloudHarvestCoreTasks.dataset import DataSet, perform_maths_operation

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
            },
            {
                'name': 'Jane Smith',
                'address': {
                    'street': '789 Oak St',
                    'city': 'Newtown',
                    'state': 'NY',
                    'zip': '11223'
                },
                'dob': '1992-07-20',
                'email': 'jane.smith2@example.com',
                'phone': '555-9876',
                'tags': ['friend'],
                'notes': 'Met at school',
                'age': 28,
                'active': True
            }
        ])

        pass

    def test_add_keys(self):
        self.dataset.add_keys(['new_key'], default_value='default')
        for record in self.dataset:
            self.assertIn('new_key', record)
            self.assertEqual(record['new_key'], 'default')

    def test_add_records(self):
        self.dataset.add_records([{'name': 'Add 1'}, {'name': 'Add 2'}])
        self.assertEqual(len(self.dataset), 5)
        self.assertEqual(self.dataset[3]['name'], 'Add 1')
        self.assertEqual(self.dataset[4]['name'], 'Add 2')

    def test_cast_key(self):
        self.dataset.cast_key('age', 'str')
        for record in self.dataset:
            self.assertIsInstance(record['age'], str)

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

    def test_copy_record(self):
        self.dataset.copy_record(source_index=0)
        self.assertEqual(len(self.dataset), 4)
        self.assertEqual(self.dataset[-1], self.dataset[0])

    def test_count_elements(self):
        self.dataset.count_elements(source_key='tags', target_key='tags_count')
        for record in self.dataset:
            self.assertIn('tags_count', record)
            self.assertEqual(record['tags_count'], len(record['tags']))

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

    def test_map(self):
        from CloudHarvestCoreTasks.dataset import WalkableDict
        self.dataset[0]['bonus_key'] = []
        expected_map = WalkableDict({
            'name': 'str',
            'address': {
                'street': 'str',
                'city': 'str',
                'state': 'str',
                'zip': 'str'
            },
            'dob': 'str',
            'email': 'str',
            'phone': 'str',
            'tags': [
                'str'
            ],
            'notes': 'str',
            'age': 'int',
            'active': 'bool',
            'bonus_key': []
        })

        self.assertEqual(self.dataset.map(), expected_map)

    def test_match_and_remove(self):
        self.dataset.match_and_remove([['name=="John Doe"']])
        for record in self.dataset:
            self.assertEqual(record['name'], 'John Doe')

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
        self.assertEqual(self.dataset.maths_results['average_age'], sum([record['age'] for record in self.dataset]) / len(self.dataset))

    def test_append_maths_results(self):
        self.dataset.maths_records(source_key='age',
                                   target_key='average_age',
                                   operation='average',
                                   missing_value=0,
                                   default_value=None)

        self.dataset.append_record_maths_results()
        self.assertEqual(self.dataset[-1]['_id'], 'Totals')

    def test_rename_keys(self):
        self.dataset.rename_keys({'name': 'full_name'})
        for record in self.dataset:
            self.assertIn('full_name', record)
            self.assertNotIn('name', record)

    def test_merge_datasets(self):
        # Originally created new method `merge_datasets` however, it was determined that add_records should work
        # the same way. This test is to ensure that add_records method works in the same way that merge_datasets
        # would have worked if it were retained.
        dataset1 = DataSet([
            {'key1': 'value1', 'key2': 'value2'},
            {'key1': 'value3', 'key2': 'value4'}
        ])

        dataset2 = DataSet([
            {'key1': 'value5', 'key2': 'value6'},
            {'key1': 'value7', 'key2': 'value8'}
        ])

        dataset3 = DataSet([
            {'key1': 'value9', 'key2': 'value10'}
        ])

        dataset1.add_records([dataset2, dataset3])

        expected_result = [
            {'key1': 'value1', 'key2': 'value2'},
            {'key1': 'value3', 'key2': 'value4'},
            {'key1': 'value5', 'key2': 'value6'},
            {'key1': 'value7', 'key2': 'value8'},
            {'key1': 'value9', 'key2': 'value10'}
        ]

        self.assertEqual(len(dataset1), 5)
        self.assertEqual(dataset1, expected_result)

    def test_merge_keys(self):
        self.dataset.merge_keys(source_keys='address', preserve_original_keys=False)

        for record in self.dataset:
            self.assertNotIn('address', record)
            self.assertIn('street', record)
            self.assertIn('city', record)
            self.assertIn('state', record)
            self.assertIn('zip', record)


    def test_nest_keys(self):
        self.dataset.nest_keys(source_keys=['name', 'dob'], target_key='person')

        for record in self.dataset:
            self.assertIn('person', record)
            self.assertIn('name', record['person'])
            self.assertIn('dob', record['person'])
            self.assertNotIn('name', record)
            self.assertNotIn('dob', record)

    def test_remove_duplicate_records(self):
        from copy import copy
        self.dataset.append(copy(self.dataset[0]))
        self.dataset.remove_duplicate_records()
        self.assertEqual(len(self.dataset), 3)

    def test_set_keys(self):
        self.dataset.set_keys(keys=['name', 'address.city'])
        for record in self.dataset:
            self.assertEqual(len(record.keys()), 2)

    def test_sort_records(self):
        # Sort by name in ascending order
        self.dataset.sort_records(keys=['name'])
        self.assertEqual(self.dataset[0]['name'], 'Jane Smith')

        # Sort by name in descending order
        self.dataset.sort_records(keys=['name:desc'])
        self.assertEqual(self.dataset[0]['name'], 'John Doe')

        # Sort by nested value in ascending order
        self.dataset.sort_records(keys=['address.city'])
        self.assertEqual(self.dataset[0].walk('address.city'), 'Anytown')

        # Sort by nested value in descending order
        self.dataset.sort_records(keys=['address.city:desc'])
        self.assertEqual(self.dataset[0].walk('address.city'), 'Othertown')

        # Sort by multiple keys, including nested keys
        self.dataset.sort_records(keys=['name', 'address.state:desc'])

        # Jane Smith of NY should be first
        self.assertEqual(self.dataset[0]['name'], 'Jane Smith') and self.assertEqual(self.dataset[0].walk('address.state'), 'NY')

        # Jane Smith of TX should be second
        self.assertEqual(self.dataset[1]['name'], 'Jane Smith') and self.assertEqual(self.dataset[1].walk('address.state'), 'TX')

        # John Doe should be last
        self.assertEqual(self.dataset[2]['name'], 'John Doe')

        # Reverse sort by multiple keys, including nested keys
        self.dataset.sort_records(keys=['name:desc', 'address.zip:desc'])
        self.assertEqual(self.dataset[0]['name'], 'John Doe')
        self.assertEqual(self.dataset[0].walk('address.zip'), '12345')

        self.assertEqual(self.dataset[1]['name'], 'Jane Smith')
        self.assertEqual(self.dataset[1].walk('address.zip'), '67890')

        self.assertEqual(self.dataset[2]['name'], 'Jane Smith')
        self.assertEqual(self.dataset[2].walk('address.zip'), '11223')

        # Resort by name and zip in ascending order
        self.dataset.sort_records(keys=['name', 'address.zip'])
        self.assertEqual(self.dataset[0]['name'], 'Jane Smith')
        self.assertEqual(self.dataset[0].walk('address.zip'), '11223')

        self.assertEqual(self.dataset[1]['name'], 'Jane Smith')
        self.assertEqual(self.dataset[1].walk('address.zip'), '67890')

        self.assertEqual(self.dataset[2]['name'], 'John Doe')
        self.assertEqual(self.dataset[2].walk('address.zip'), '12345')

        # Resort name:desc and zip:asc
        self.dataset.sort_records(keys=['name:desc', 'address.zip'])
        self.assertEqual(self.dataset[0]['name'], 'John Doe')
        self.assertEqual(self.dataset[0].walk('address.zip'), '12345')

        self.assertEqual(self.dataset[1]['name'], 'Jane Smith')
        self.assertEqual(self.dataset[1].walk('address.zip'), '11223')

        self.assertEqual(self.dataset[2]['name'], 'Jane Smith')
        self.assertEqual(self.dataset[2].walk('address.zip'), '67890')

    def split_key_to_keys(self):
        self.dataset.split_key_to_keys(
            source_key='address.street',
            target_keys=['address.street_number', 'address.street_name', 'address.street_type'],
            separator=' ',
            preserve_source_key=True
        )

        for record in self.dataset:
            self.assertIn('address.street_number', record)
            self.assertIn('address.street_name', record)
            self.assertIn('address.street_type', record)
            self.assertEqual(record['address.street_number'], record['address.street'].split(' ')[0])
            self.assertEqual(record['address.street_name'], record['address.street'].split(' ')[0])
            self.assertEqual(record['address.street_type'], record['address.street'].split(' ')[0])

    def test_unwind_and_wind(self):
        self.dataset.unwind('tags')
        self.assertEqual(len(self.dataset), 4)
        for record in self.dataset:
            self.assertIsInstance(record['tags'], str)

        self.dataset.wind('tags')

        self.assertEqual(len(self.dataset), 3)
        for record in self.dataset:
            self.assertIsInstance(record['tags'], list)

    def test_create_index(self):
        self.dataset.create_index(name='address_state_index', keys=['address.state'])

        self.assertEqual(self.dataset.indexes['address_state_index']['keys'], ['address.state'])
        self.assertEqual(list(self.dataset.indexes['address_state_index']['values'].keys()), ['CA', 'TX', 'NY'])

        for index_definition, records in self.dataset.indexes['address_state_index']['values'].items():
            self.assertEqual(len(records), 1)
            self.assertEqual(index_definition, records[0].walk('address.state'))

    def test_drop_index(self):
        self.dataset.create_index(name='address_state_index', keys=['address.state'])
        self.dataset.drop_index('address_state_index')

        self.assertNotIn('address_state_index', self.dataset.indexes)

    def test_find_index(self):
        self.dataset.create_index(name='address_state_index', keys=['address.state'])
        index = self.dataset.find_index(keys=['address.state'])

        self.assertEqual(index, 'address_state_index')

    def test_refresh_index(self):
        self.dataset.create_index(name='address_state_index', keys=['address.state'])

        self.dataset[0].assign('address.state', 'TX')
        self.dataset.refresh_index('address_state_index')

        self.assertEqual(len(self.dataset.indexes['address_state_index']['values']), 2)
        self.assertEqual(list(self.dataset.indexes['address_state_index']['values'].keys()), ['TX', 'NY'])

    def test_inner_join(self):
        righthand_dataset = DataSet([
            {
                'name': 'John Doe',
                'dob': '1990-01-01',
                'medications': ['med-1, med-2']
            },
            {
                'name': 'Jane Smith',
                'dob': '1985-05-15',
                'medications': ['med-3']
            },
            # intentionally not included for inner join test
            # {
            #     'name': 'Jane Smith',
            #     'dob': '1992-07-20',
            # }

        ])

        self.dataset.join(righthand_dataset, left_keys=['name', 'dob'], right_keys=['name', 'dob'], inner=True)

        self.assertEqual(len(self.dataset), 2)
        for record in self.dataset:
            self.assertIn('medications', record)
            self.assertIsInstance(record['medications'], list)
            self.assertEqual(record['medications'], ['med-1, med-2'] if record['name'] == 'John Doe' else ['med-3'])

    def test_join(self):
        righthand_dataset = DataSet([
            {
                'name': 'John Doe',
                'dob': '1990-01-01',
                'medications': ['med-1, med-2']
            },
            {
                'name': 'Jane Smith',
                'dob': '1985-05-15',
                'medications': ['med-3']
            },
            # intentionally not included for inner join test
            # {
            #     'name': 'Jane Smith',
            #     'dob': '1992-07-20',
            # }

        ])

        self.dataset.join(righthand_dataset, left_keys=['name', 'dob'], right_keys=['name', 'dob'], inner=False)

        self.assertEqual(len(self.dataset), 3)
        self.assertEqual(self.dataset[0]['medications'], ['med-1, med-2'])
        self.assertEqual(self.dataset[1]['medications'], ['med-3'])
        self.assertNotIn('medications', self.dataset)

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

    def test_map(self):
        expected_map = {
            'name': 'str',
            'address': {
                'street': 'str',
                'city': 'str',
                'state': 'str',
                'zip': 'str'
            },
            'dob': 'str',
            'email': 'str',
            'phone': 'str',
            'tags': [
                'str'
            ],
            'notes': 'str',
            'age': 'int',
            'active': 'bool'
        }
        for record in self.dataset:
            self.assertEqual(record.map(), expected_map)

    def test_replace(self):
        from CloudHarvestCoreTasks.dataset import WalkableDict
        test_dict = WalkableDict({
            'whole_string_replacement': 'var.test-string',
            'substring_replacement': 'my var.test-string is here',
            'multi_var_replacement': 'var.test-string var.test-string-2',
            'nested_substring_replacement': [
                {'nested_key': 'my var.test-string is here'},
                'var.test-dict'
            ],
            'no_change': 'this should not change',
            'no_replacement_var': 'var.does-not-exist',
        })

        result = test_dict.replace(variables={'var': {'test-string': 'replaced', 'test-string-2': 'replaced2', 'test-dict': {'key': 'value'}}})

        self.assertEqual(result['whole_string_replacement'], 'replaced')
        self.assertEqual(result['substring_replacement'], 'my replaced is here')
        self.assertEqual(result['multi_var_replacement'], 'replaced replaced-2')
        self.assertEqual(result['nested_substring_replacement'][0]['nested_key'], 'my replaced is here')
        self.assertEqual(result['nested_substring_replacement'][1], {'key': 'value'})
        self.assertEqual(result['no_change'], 'this should not change')
        self.assertEqual(result['no_replacement_var'], 'var.does-not-exist')

    def test_walk(self):
        for record in self.dataset:
            self.assertEqual(record.walk('address.city'), record['address']['city'])

        self.dataset[0]['test_nested_int_as_key'] = {'123456789012': {'nested_value': 'test'}}

        self.assertEqual(self.dataset[0].walk('test_nested_int_as_key.123456789012.nested_value'), 'test')

    def test_assign(self):
        # Test basic nested assignment of an existing value
        for record in self.dataset:
            record.assign('address.city', 'New City')
            self.assertEqual(record['address']['city'], 'New City')

        # Test nested assignment of a missing path
        for record in self.dataset:
            record.assign('address.county', 'New County')
            self.assertEqual(record['address']['county'], 'New County')

        # Test a deeply nested assignment on a path which does not exist
        for record in self.dataset:
            record.assign('do.you.like.testing.nested.objects.i.do', 'Yes')
            self.assertEqual(record['do']['you']['like']['testing']['nested']['objects']['i']['do'], 'Yes')

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
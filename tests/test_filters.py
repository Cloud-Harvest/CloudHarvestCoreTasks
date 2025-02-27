import unittest

from ..CloudHarvestCoreTasks.filters import DataSetFilter, MongoFilter, SqlFilter
from ..CloudHarvestCoreTasks.dataset import DataSet

class TestMatch(unittest.TestCase):
    def setUp(self):
        from ..CloudHarvestCoreTasks.filters import Match
        self.match = Match('user=1|2')

    def test_match(self):
        self.assertEqual(self.match.key, 'user')
        self.assertEqual(self.match.value, '1|2')
        self.assertEqual(self.match.operator, '=')
        self.assertEqual(self.match.as_str(), 'user=1|2')

class TestMatchSet(unittest.TestCase):
    def setUp(self):
        from ..CloudHarvestCoreTasks.filters import MatchSet

        self.match_set = MatchSet(*['user=1|2', 'state=MD'])

    def test_match_set(self):
        from ..CloudHarvestCoreTasks.filters import Match

        self.assertEqual(len(self.match_set), 2)
        self.assertIsInstance(self.match_set[0], Match)
        self.assertIsInstance(self.match_set[1], Match)

        self.assertEqual(self.match_set.as_dict(), {'$and': ['user=1|2', 'state=MD']})

class TestMatchSetGroup(unittest.TestCase):
    def setUp(self):
        from ..CloudHarvestCoreTasks.filters import MatchSetGroup

        self.match_set_group = MatchSetGroup(*[
            ['user=1|2', 'state=MD'],       # user will be 1 or 2 AND state will be MD
            ['user=3', 'state=VA']          # OR user will be 3 AND state will be VA
        ])

    def test_match_set_group(self):
        from ..CloudHarvestCoreTasks.filters import MatchSet

        self.assertEqual(len(self.match_set_group), 2)
        self.assertIsInstance(self.match_set_group[0], MatchSet)
        self.assertIsInstance(self.match_set_group[1], MatchSet)

        self.assertEqual(self.match_set_group.as_dict(), {
            '$or': [
                {
                    '$and': [
                        'user=1|2',
                        'state=MD'
                    ]
                },
                {
                    '$and': [
                        'user=3',
                        'state=VA'
                    ]
                }
            ]
        })

class TestDataSetFilter(unittest.TestCase):
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

        self.filter = DataSetFilter(
            dataset=self.dataset,
            accepted='.*',
            add_keys=['test_add_key'],
            exclude_keys=['tags'],
            headers=['name', 'address.state', 'dob'],
            matches=[['address.state=CA|TX']],
            sort=['name']
        )


    def test_filter(self):
        self.filter.apply()

        # Make sure the new key is added to all records
        [
            self.assertIn('test_add_key', record)
            for record in self.filter.result
        ]

        # Make sure the data key is removed from all records
        [
            self.assertNotIn('tags', record)
            for record in self.filter.result
        ]

        # Make sure the headers are correct
        for record in self.filter.result:
            self.assertEqual(len(record.keys()), 4)

        # Make sure the matches are correct
        self.assertEqual(len(self.filter.result), 2)

        # Make sure the sorting is correct
        self.assertEqual(self.filter.result[0].walk('name'), 'Jane Smith')
        self.assertEqual(self.filter.result[0].walk('address.state'), 'TX')
        self.assertEqual(self.filter.result[1].walk('name'), 'John Doe')
        self.assertEqual(self.filter.result[1].walk('address.state'), 'CA')

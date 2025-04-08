from CloudHarvestCoreTasks.cache import CachedData

import unittest

class TestCachedData(unittest.TestCase):
    def setUp(self):
        self.cached_data = CachedData(data={'key': 'value'}, valid_age=3)

    def test_initialization(self):
        self.assertEqual(self.cached_data.data, {'key': 'value'})
        self.assertEqual(self.cached_data.valid_age, 3)

    def test_age(self):
        from time import sleep
        while self.cached_data.age < 3:
            self.assertTrue(self.cached_data.is_valid)

            sleep(.5)

        self.assertFalse(self.cached_data.is_valid)

    def test_update(self):
        self.cached_data.update(data={'new_key': 'new_value'}, valid_age=5)
        self.assertEqual(self.cached_data.data, {'new_key': 'new_value'})
        self.assertEqual(self.cached_data.valid_age, 5)

        from time import sleep
        while self.cached_data.age < 5:
            self.assertTrue(self.cached_data.is_valid)

            sleep(.5)

        self.assertFalse(self.cached_data.is_valid)

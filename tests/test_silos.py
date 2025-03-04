from unittest import TestCase
from CloudHarvestCoreTasks.silos import *


class test_MongoSilo(TestCase):
    def setUp(self):
        self.silo = MongoSilo(name='test',
                              engine='mongo',
                              host='localhost',
                              port=27017,
                              database='test',
                              username='admin',
                              password='default-harvest-password',
                              authSource='admin',
                              timeout=250)

    def test_connect(self):
        self.silo.connect()
        self.assertTrue(self.silo.is_connected)

    def test_add_indexes(self):
        self.silo.add_indexes(
            {
                'test_collection': [
                    'test_simple_index',
                    {
                        'keys': [
                            {'field': 'complex_text_field_one'},
                            {'field': 'complex_text_field_two'}
                        ],
                        'options': {
                            'name': 'test_complex_index',
                            'comment': 'This is a test complex index',
                            'unique': True
                        }
                    }
                ]

            }
        )

        # Validate the index by querying the database
        from pymongo import MongoClient
        client: MongoClient = self.silo.connect()

        # Test the simple index
        database_index_information = client[self.silo.database]['test_collection'].index_information()

        pass
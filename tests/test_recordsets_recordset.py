import unittest
from ..CloudHarvestCoreTasks.data_model import recordset

HarvestRecord = recordset.HarvestRecord
HarvestRecordSet = recordset.HarvestRecordSet
HarvestRecordSets = recordset.HarvestRecordSets


class TestHarvestRecordSet(unittest.TestCase):
    def setUp(self):
        self.recordset = HarvestRecordSet(data=[{'index': i, 'value': f'value_{i}'} for i in range(5)])

    def test_add(self):
        self.recordset.add(data=[{'index': 5, 'value': 'value_5'}])
        self.assertEqual(len(self.recordset), 6)

    def test_create_index(self):
        # Create a recordset with 10 records, each record has 'index' and 'value' fields
        self.recordset = HarvestRecordSet(data=[{'index': i, 'value': f'value_{i}'} for i in range(10)])
        # Create an index named 'index1' based on the 'index' field
        self.recordset.create_index('index1', 'index')
        self.assertEqual(len(self.recordset.indexes['index1']), 10)  # Assert that the index includes all 10 records

        # Create another index named 'index2' based on both 'index' and 'value' fields
        self.recordset.create_index('index2', 'index', 'value')
        # Assert that the index includes all 10 records
        self.assertEqual(len(self.recordset.indexes['index2']), 10)
        # Assert that the index is correctly created based on both 'index' and 'value' fields
        self.assertEqual(self.recordset.indexes['index2'][(0, 'value_0')], [{'index': 0, 'value': 'value_0'}])

    def test_drop_index(self):
        self.recordset.create_index('index1', 'index')
        self.recordset.drop_index('index1')
        self.assertEqual('index1' in self.recordset.indexes, False)

    def test_rebuild_indexes(self):
        self.recordset.create_index('index1', 'index')
        self.assertEqual(len(self.recordset.indexes['index1']), 5)

        self.recordset.add(data=[{'index': 5, 'value': 'value_5'}])
        self.recordset.rebuild_indexes()
        self.assertEqual(len(self.recordset.indexes['index1']), 6)

    def test_remove_duplicates(self):
        self.recordset.add(data=[{'index': 1, 'value': 'value_1'}])
        self.recordset.remove_duplicates()
        self.assertEqual(len(self.recordset), 5)

    def test_unwind(self):
        self.recordset.add(data=[{'index': 5, 'value': ['value_5', 'value_6']}])
        self.recordset.unwind(source_key='value')
        self.assertEqual(len(self.recordset), 7)


class TestHarvestRecordSets(unittest.TestCase):
    def setUp(self):
        self.recordsets = HarvestRecordSets()
        data1 = [{'index': i, 'value': f'value_{i}'} for i in range(5)]
        data2 = [{'index': i, 'value': f'value_{i}'} for i in range(3, 8)]
        self.recordsets.add('recordset1', HarvestRecordSet(data=data1))
        self.recordsets.add('recordset2', HarvestRecordSet(data=data2))
        self.recordsets.index('recordset1', 'index1', 'index')
        self.recordsets.index('recordset2', 'index2', 'index')
        self.recordsets.index('recordset1', 'index', 'index')
        self.recordsets.index('recordset2', 'index', 'index')

    def test_add(self):
        self.recordsets.add('recordset3', HarvestRecordSet(data=[{'index': 0, 'value': 'value_0'}]))
        self.assertEqual(len(self.recordsets['recordset3']), 1)

    def test_index(self):
        self.recordsets.index('recordset1', 'index3', 'value')
        self.assertEqual(len(self.recordsets['recordset1'].indexes['index3'][(None, )]), 5)

    # def test_join(self):
    #     # Test inner join
    #     self.recordsets.join('joined_inner', ['recordset1', 'recordset2'], 'index', 'inner')
    #     self.assertEqual(len(self.recordsets['joined_inner']), 2)
    #
    #     # Test outer join
    #     self.recordsets.join('joined_outer', ['recordset1', 'recordset2'], 'index', 'outer')
    #     self.assertEqual(len(self.recordsets['joined_outer']), 7)
    #
    #     # Test left join
    #     self.recordsets.join('joined_left', ['recordset1', 'recordset2'], 'index', 'left')
    #     self.assertEqual(len(self.recordsets['joined_left']), 5)
    #
    #     # Test right join
    #     self.recordsets.join('joined_right', ['recordset1', 'recordset2'], 'index', 'right')
    #     self.assertEqual(len(self.recordsets['joined_right']), 5)

    def test_list(self):
        recordset_list = self.recordsets.list()
        self.assertEqual(len(recordset_list), 2)

    def test_purge(self):
        self.recordsets.purge()
        self.assertEqual(len(self.recordsets), 0)

    def test_query(self):
        queried_recordset = self.recordsets.query('recordset1')
        self.assertEqual(len(queried_recordset), 5)

    def test_remove(self):
        self.recordsets.remove('recordset1')
        self.assertEqual('recordset1' in self.recordsets, False)

    def test_rename(self):
        self.recordsets.rename('recordset1', 'recordset_renamed')
        self.assertEqual('recordset1' in self.recordsets, False)
        self.assertEqual('recordset_renamed' in self.recordsets, True)

    def test_union(self):
        self.recordsets.union('union_recordset', ['recordset1', 'recordset2'])
        self.assertEqual(len(self.recordsets['union_recordset']), 10)


if __name__ == '__main__':
    unittest.main()

from typing import Dict, List, Literal
from .record import HarvestRecord


class HarvestRecordSet(List[HarvestRecord]):
    def __init__(self, name: str = None, data: List[Dict] = None, **kwargs):
        """
        Initialize a HarvestRecordSet object.

        :param data: A list of dictionaries to initialize the record set with, defaults to None
        :param kwargs: Additional keyword arguments
        """

        super().__init__(**kwargs)

        from uuid import uuid4
        self.name = name or str(uuid4())

        self.indexes = {}
        self.index_fields = {}

        if data:
            self.add(data=data)

    def __enter__(self) -> 'HarvestRecordSet':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def __add__(self, other):
        self.add(data=other)

        return self

    @property
    def keys(self) -> List[str]:
        return sorted(list(set([key for record in self for key in record.keys()])))

    def add(self, data: (List[dict or HarvestRecord]) or dict or HarvestRecord) -> 'HarvestRecordSet':
        """
        Add a list of records to the record set.

        This method accepts a list of dictionaries or HarvestRecord objects, a single dictionary, or a single HarvestRecord object.
        If the input is a dictionary, it will be automatically converted to a HarvestRecord.
        If the input is a HarvestRecord, it will be directly appended to the record set.
        If the input is a list, each element in the list will be appended to the record set. If an element is a dictionary, it will be converted to a HarvestRecord.

        After adding the new data, the indexes of the record set are rebuilt.

        Args:
            data (Union[List[Union[dict, 'HarvestRecord']], dict, 'HarvestRecord']): A list of dictionaries or HarvestRecord objects to add to the record set. If the object is a dictionary, it will automatically be converted to a HarvestRecord.

        Returns:
            HarvestRecordSet: The current HarvestRecordSet instance.
        """

        if isinstance(data, dict):
            self.append(HarvestRecord(recordset=self, **data))

        elif isinstance(data, HarvestRecord):
            data.recordset = self
            self.append(data)

        elif isinstance(data, list):
            [
                self.add(item)
                for item in data
            ]

        self.rebuild_indexes()

        return self

    def add_match(self, syntax: str) -> 'HarvestRecordSet':
        """
        Add a match to the record set.

        :param syntax: The match syntax to add
        """

        [record.match(syntax) for record in self]

        return self

    def clear_matches(self) -> 'HarvestRecordSet':
        """
        Clear all matches from the record set.
        """

        [record.clear_matches() for record in self]

        return self

    def create_index(self, index_name: str, *fields) -> 'HarvestRecordSet':
        """
        Create an index for the record set.

        :param index_name: The name of the index
        :param fields: The fields to include in the index

        """

        index = {}
        for record in self:
            # Sometimes a dictionary may not have the associated field. In this case, we will use None as the value.
            key = tuple(record.get(field) for field in fields)

            if key in index:
                index[key].append(record)

            else:
                index[key] = [record]

        self.indexes[index_name] = index
        self.index_fields[index_name] = fields

        return self

    def drop_index(self, index_name: str) -> 'HarvestRecordSet':
        """
        Drop an index from the record set.

        :param index_name: The name of the index to drop
        """

        self.indexes.pop(index_name)

        return self

    def get_matched_records(self) -> 'HarvestRecordSet':
        """
        Get all records in the record set that are a match.

        :return: A HarvestRecordSet object containing all matched records
        """

        return HarvestRecordSet(data=[record for record in self if record.is_matched_record])

    def modify_records(self, function: str, arguments: dict) -> 'HarvestRecordSet':
        """
        Modify records in the record set by calling a function on each record.

        :param function: The name of the function to call
        :param arguments: The arguments to pass to the function
        """

        [getattr(record, function)(**arguments) for record in self]

        self.rebuild_indexes()

        return self

    def rebuild_indexes(self):
        """
        Rebuild all indexes for the record set.
        """

        self.indexes.clear()
        for index_name, fields in self.index_fields.items():
            self.create_index(index_name, *fields)

        return self

    def remove_duplicates(self) -> 'HarvestRecordSet':
        """
        Remove duplicate records from the record set.
        """

        unique_records = {
            frozenset(record.items()): record
            for record in self
        }

        self.clear()
        self.add(data=list(unique_records.values()))

        self.rebuild_indexes()

        return self

    def remove_unmatched_records(self) -> 'HarvestRecordSet':
        """
        Remove all records in the record set that are not a match.
        """

        self[:] = [
            record for record in self
            if record.is_matched_record
        ]

        self.rebuild_indexes()

        return self

    def unwind(self, source_key: str, preserve_null_and_empty_keys: bool = True) -> 'HarvestRecordSet':
        """
        Unwind a list of records in the record set into separate records.

        :param source_key: The key of the list to unwind
        :param preserve_null_and_empty_keys: Whether to preserve keys with null or empty values, defaults to True
        """

        new_records = []
        for record in self:
            if source_key not in record and preserve_null_and_empty_keys is False:
                continue

            elif isinstance(record[source_key], (list or tuple)):
                for item in record[source_key]:
                    new_record = record.copy()
                    new_record[source_key] = item
                    new_records.append(new_record)

            else:
                new_records.append(record)

        self.clear()
        self.add(data=new_records)

        return self


class HarvestRecordSets(Dict[str, HarvestRecordSet]):

    def add(self, recordset_name: str, recordset: HarvestRecordSet) -> 'HarvestRecordSets':
        self[recordset_name] = recordset

        return self

    def index(self, recordset_name: str, index_name: str, *fields) -> 'HarvestRecordSets':
        self[recordset_name].create_index(index_name, fields)

        return self

    # TODO: implement join() of two or more HarvestRecordSets
    # def join(self, new_recordset_name: str, recordset_names: List[str], index_name: str,
    #          join_type: Literal['inner', 'outer', 'left', 'right']) -> 'HarvestRecordSets':
    #
    #     # Retrieve the recordsets
    #     recordsets = [self[recordset_name] for recordset_name in recordset_names]
    #
    #     # Retrieve the indexes
    #     indexes = [recordset.indexes[index_name] for recordset in recordsets if recordset.indexes.get(index_name)]
    #
    #     # Perform the join operation
    #     match join_type:
    #         case 'inner':
    #             joined_data = []
    #             for index in indexes:
    #                 joined_data.extend(index)
    #
    #
    #         case 'outer':
    #             joined_data = set.union(*map(set, indexes))
    #
    #         case 'left':
    #             joined_data = set(indexes[0]).union(*indexes[1:])
    #
    #         case 'right':
    #             joined_data = set(indexes[-1]).union(*indexes[:-1])
    #
    #         case _:
    #             raise ValueError('Invalid join type')
    #
    #     # Create a new recordset with the joined data
    #     new_recordset = HarvestRecordSet(data=joined_data)
    #
    #     # Add the new recordset to the dictionary
    #     self[new_recordset_name] = new_recordset
    #
    #     return self

    def list(self) -> List[dict]:
        return [
            {
                'Name': name,
                'Keys': '\n'.join(recordset.keys),
                'Matches': len(recordset.get_matched_records()),
                'Total': len(recordset)
            }
            for name, recordset in self.items()
        ]

    def purge(self) -> 'HarvestRecordSets':
        self.clear()

        return self

    def query(self, recordset_name: str):
        return self.get(recordset_name)

    def remove(self, name: str) -> 'HarvestRecordSets':
        self.pop(name)
        return self

    def rename(self, old_recordset_name: str, new_recordset_name: str) -> 'HarvestRecordSets':
        self[new_recordset_name] = self.pop(old_recordset_name)

        return self

    def union(self, new_recordset_name: str, recordset_names: List[str]) -> 'HarvestRecordSets':
        new_recordset = HarvestRecordSet()
        [new_recordset.add(data=self[recordset_name]) for recordset_name in recordset_names]

        self[new_recordset_name] = new_recordset

        return self

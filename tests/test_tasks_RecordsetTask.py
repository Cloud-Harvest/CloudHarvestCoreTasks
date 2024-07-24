import unittest
from ..CloudHarvestCoreTasks.data_model.recordset import HarvestRecordSet
from datetime import datetime

test_task_template = {
    "name": "test_chain",
    "description": "This is a test chain.",
    "tasks": [
        {
            "recordset": {
                "name": "test recordset task",
                "description": "This is a test record set",
                "recordset_name": "test_recordset",
                "stages": [
                    {
                        "key_value_list_to_dict": {
                            "source_key": "tags",
                            "target_key": "tags_dict",
                            "name_key": "Name"
                        }
                    },
                    {
                        "copy_key": {
                            "source_key": "age",
                            "target_key": "age_copy"
                        }
                    }

                ],
                "results_as": "result"
            }
        }

    ]

}

test_data = [
    {
        "name": "Test1",
        "age": 30,
        "date": datetime.now(),
        "tags": [{"Name": "color", "Value": "blue"}, {"Name": "size", "Value": "large"}]
    },
    {
        "name": "Test2",
        "age": 25,
        "date": datetime.now(),
        "tags": [{"Name": "color", "Value": "red"}, {"Name": "size", "Value": "medium"}]
    }
]


class TestRecordSetTask(unittest.TestCase):
    def setUp(self):
        # import required to register class
        from ..CloudHarvestCoreTasks.tasks import HarvestRecordSetTask

        self.recordset = HarvestRecordSet(data=test_data)

        from ..CloudHarvestCoreTasks.base import BaseTaskChain
        self.chain = BaseTaskChain(template=test_task_template)
        self.chain.variables["test_recordset"] = self.recordset

    def test_init(self):
        self.assertEqual(self.chain.variables["test_recordset"], self.recordset)
        self.assertEqual(self.chain.task_templates[0].name, "test recordset task")

    def test_method(self):
        self.chain.run()
        result = self.chain.result
        self.assertEqual(result["data"][0]["tags_dict"], {"color": "blue", "size": "large"})
        self.assertEqual(result["data"][1]["tags_dict"], {"color": "red", "size": "medium"})
        [
            self.assertEqual(record["age"], record["age_copy"])
            for record in result["data"]
        ]


if __name__ == '__main__':
    unittest.main()

# HarvestRecordSetTask
The `HarvestRecordSetTask` class is a subclass of the `BaseTask` class. It represents a task that modifies a record set by applying functions to its records.

> Note: For detailed documentation on the Harvest Recordset and its functions, please review [this documentation](../../CloudHarvestCoreTasks/data_model/README.md).

# Python

## Attributes
| Attribute           | Description                                                                                                                                             |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| **recordset_name**  | The name of the record set this task operates on.                                                                                                       |
| **stages**          | A list of dictionaries containing the function name and arguments to be applied to the recordset. Stages are performed in the order they were provided. |
| **record_position** | The position of the record in the recordset.                                                                                                            |

## Methods
| Method       | Description                                                                                                                 |
|--------------|-----------------------------------------------------------------------------------------------------------------------------|
| **method()** | Executes functions on the recordset with the provided function and arguments, then stores the result in the data attribute. |

## Code Example
```python
from CloudHarvestCoreTasks.tasks import HarvestRecordSetTask

stages = [{'function_name': {'argument1': 'value1'}}]
recordset_task = HarvestRecordSetTask(in_data='previous_data', stages=stages)
recordset_task.method()
print(recordset_task.out_data)  # Output: Modified recordset
```

# Configuration

## Arguments
The `HarvestRecordSetTask` class has the following arguments beyond those defined in [BaseTask](./base.md). Furthermore,
information on the available functions and their arguments can be found in the [Harvest Recordset documentation](../../CloudHarvestCoreTasks/data_model/README.md).

| Key            | Required | Default | Description                                                                                                    |
|----------------|----------|---------|----------------------------------------------------------------------------------------------------------------|
| stages         | Yes      | None    | A list of dictionaries containing the function name and arguments to be applied to the recordset sequentially. |

## Example

```yaml
recordset:
  name: My Record Set Task
  description: A task that operates on a record set.
  in_data: previous_task_data
  stages:
    - function_name:
        argument1: value1
        argument2: value2
  record_position: 0
```
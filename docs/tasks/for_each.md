# ForEachTask
This task is used when you want to create new tasks based on the outcome of a previous task. For example, you may want to create a new task for each record in a list of records.

# Attributes
| Attribute                    | Description                                                               |
|------------------------------|---------------------------------------------------------------------------|
| **template**                 | A dictionary representing the Task template to be applied to each record. |
| **records**                  | A list of records or the name of a variable containing the records.       |
| **insert_tasks_at_position** | The position at which to insert the tasks.                                |
| **insert_tasks_before_name** | The name of the task before which to insert the tasks.                    |
| **insert_tasks_after_name**  | The name of the task after which to insert the tasks.                     |

# Methods
| Method       | Description                                                        |
|--------------|--------------------------------------------------------------------|
| **method()** | Iterates over the records and applies the template to each record. |

# Example

## Configuration
```yaml
for:
    name: A ForEach Task
    records:
        - {key1: value1a, key2: value2a}
        - {key1: value1b, key2: value2b}
    template:
        file:
          name: My Templated FileTask
          description: This is a templated FileTask
          path: '/path/to/{{record.key}}-file.log'
          mode: write
          with_vars: record
```

## Python
```python
from CloudHarvestCoreTasks.tasks import ForEachTask

template = {'task': 'dummy_task'}
records = [{'record1': 'value1'}, {'record2': 'value2'}]
for_each_task = ForEachTask(template=template, records=records)
for_each_task.method()
```
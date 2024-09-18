# ForEachTask
This task is used when you want to create new tasks based on the outcome of a previous task.

For example, you may want to create a new task for each record in a list of records.

# Table of Contents

- [ForEachTask](#foreach-task)
- [Python](#python)
    - [Attributes](#attributes)
        - [in\_data Formatting](#in_data-formatting)
    - [Methods](#methods)
    - [Code Example](#code-example)
- [Configuration](#configuration)
    - [Arguments](#arguments)
    - [Example](#example)

# Python
## Attributes
| Attribute                    | Description                                                                                                                                                                            |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **template**                 | A dictionary representing the Task template to be applied to each record.                                                                                                              |
| **in_data**                  | A string, list of objects, or list of dictionaries which will be the basis for creating new Tasks. `ForEachTask` will automatically populate the `in_data` field for Tasks it creates. |
| **insert_tasks_at_position** | The position at which to insert the tasks.                                                                                                                                             |
| **insert_tasks_before_name** | The name of the task before which to insert the tasks.                                                                                                                                 |
| **insert_tasks_after_name**  | The name of the task after which to insert the tasks.                                                                                                                                  |

## in_data Formatting
The `in_data` attribute can be a string, list of objects, or list of dictionaries. Since this attribute can be of many
types, the `ForEachTask` will handle each type differently.

| Type                 | Description                                                                                                                                                        |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| String               | The string will be treated as a TaskChain variable name and the value of the variable will be used as the `in_data`.                                               |
| List of Dictionaries | Each dictionary in the list will be treated as a record. Fields in the `ForEachTask` can be referenced by name using `{{var_name}}`.                               |
| List of Objects      | Each object in the List will be converted to a single-key dictionary. The new key will be `item`. The original value can therefore be referenced using `{{item}}`. |


## Methods
| Method       | Description                                                        |
|--------------|--------------------------------------------------------------------|
| **method()** | Iterates over the records and applies the template to each record. |

## Code Example

```python
from CloudHarvestCoreTasks.tasks import ForEachTask

template = {'task': 'dummy_task {{name}}'}
records = [{'name': 'value1'}, {'name': 'value2'}]
for_each_task = ForEachTask(template=template, records=records)
for_each_task.run()

# Creates and executes two tasks with the following names:
# - dummy_task value1
# - dummy_task value2
```

# Configuration

## Arguments
The `ForEachTask` class has the following arguments beyond those defined in [BaseTask](./base_task).

| Key                      | Required | Default | Description                                                                                        |
|--------------------------|----------|---------|----------------------------------------------------------------------------------------------------|
| template                 | Yes      | None    | A dictionary representing the Task template to be applied to each record.                          |
| insert_tasks_at_position | No       | None    | The position at which to insert the tasks.                                                         |
| insert_tasks_before_name | No       | None    | The name of the task before which to insert the tasks.                                             |
| insert_tasks_after_name  | No       | None    | The name of the task after which to insert the tasks.                                              |

## Example
```yaml
for_each:
  name: A ForEach Task
  in_data:
    - {key1: value1a, key2: value2a}
    - {key1: value1b, key2: value2b}
  template:
    file:
      name: My Templated FileTask {{key2}}
      description: This is a templated FileTask
      path: '/path/to/{{key1}}-file.log'
      mode: write

# Creates two tasks with the following names and paths:
# - "My Templated FileTask value2a", path: /path/to/value1a-file.log
# - "My Templated FileTask value2b", path: /path/to/value1b-file.log
```

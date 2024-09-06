# FileTask
The `FileTask` class is a subclass of the `BaseTask` class. It represents a task that performs file operations such as reading from or writing to a file.

# Table of Contents

- [FileTask](#filetask)
- [Python](#python)
    - [Attributes](#attributes)
    - [Methods](#methods)
    - [Code Examples](#code-examples)
- [Configuration](#configuration)
    - [Arguments](#arguments)
    - [Examples](#examples)
        - [Reading Files](#reading-files)
        - [Writing Files](#writing-files)

# Python

## Attributes
| Attribute        | Description                                                           |
|------------------|-----------------------------------------------------------------------|
| **path**         | The path to the file.                                                 |
| **abs_path**     | The absolute path to the file resolved from the provided `path`.      |
| **in_data**      | Data to write to a file. Inherited from `BaseTask`.                   |
| **mode**         | The mode in which the file will be opened (append, read, write).      |
| **format**       | The format of the file (`config`, `csv`, `json`, `raw`, `yaml`).      |
| **desired_keys** | When specified, only the keys identified will be written to the file. |
| **template**     | A `jinja2` template to use when writing data.                         |
| **result_as**    | The name under which a `read` result will be stored.                  |

## Methods
| Method                 | Description                                                              |
|------------------------|--------------------------------------------------------------------------|
| **determine_format()** | Determines the format of the file based on its extension.                |
| **method()**           | Performs the file operation specified by the mode and format attributes. |

## Code Examples

```python
from CloudHarvestCoreTasks.tasks import FileTask
task = FileTask(path='data.json', mode='read', result_as='result')
task.run()
print(task.out_data)  # Output: Content of the file
```

# Configuration

## Arguments
The `FileTask` class has the following arguments beyond those defined in [BaseTask](./base.md).

| Key          | Required | Default | Description                                                           |
|--------------|----------|---------|-----------------------------------------------------------------------|
| path         | Yes      | None    | The path to the file.                                                 |
| mode         | Yes      | None    | The mode in which the file will be opened (append, read, write).      |
| format       | No       | None    | The format of the file (`config`, `csv`, `json`, `raw`, `yaml`).      |
| desired_keys | No       | None    | When specified, only the keys identified will be written to the file. |
| template     | No       | None    | A `jinja2` template to use when writing data.                         |
| result_as    | No       | None    | The name under which a `read` result will be stored.                  |

## Examples
### Reading Files

```yaml
file:
  name: My File Task
  description: A task that reads from a file.
  path: ./data.json
  mode: read
  result_as: result
```

### Writing Files
If `in_data` os a string, it will be treated as a variable name and the value of the variable will be retrieved from the TaskChain as though the user specified `with_vars`.

```yaml
file:
  name: My File Task
  description: A task that writes to a file.
  path: ./data.json
  mode: write
  in_data: [{"name": "John Doe", "age": 30}, {"name": "Jane Doe", "age": 25}] 
```

# Python
```python
from CloudHarvestCoreTasks.tasks import FileTask

file_task = FileTask(path='data.json', result_as='result', mode='read')
file_task.method()
print(file_task.data)  # Output: Content of the file
```
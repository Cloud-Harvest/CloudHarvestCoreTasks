# FileTask
The `FileTask` class is a subclass of the `BaseTask` class.

It represents a task that performs file operations such as reading from or writing to a file.

# Attributes
| Attribute        | Description                                                      |
|------------------|------------------------------------------------------------------|
| **path**         | The path to the file.                                            |
| **abs_path**     | The absolute path to the file.                                   |
| **data**         | Data to write to a file.                                         |
| **mode**         | The mode in which the file will be opened (append, read, write). |
| **format**       | The format of the file (config, csv, json, raw, yaml).           |
| **desired_keys** | A list of keys to filter the data by.                            |
| **template**     | A template to use for the output.                                |
| **result_as**    | The name under which a `read` result will be stored.             |

# Methods
| Method                 | Description                                                              |
|------------------------|--------------------------------------------------------------------------|
| **determine_format()** | Determines the format of the file based on its extension.                |
| **method()**           | Performs the file operation specified by the mode and format attributes. |

# Examples

## Configuration

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
> Note: The `with_vars` attribute (inherited from `BaseTask`) is necessary to specify the file contents to write. Recall
> that `with_vars` the name of a variable assigned in a previous task using `result_as`.

```yaml
file:
    name: My File Task
    description: A task that writes to a file.
    path: ./data.json
    mode: write
    with_vars: previous_result
```

# Python
```python
from CloudHarvestCoreTasks.tasks import FileTask

file_task = FileTask(path='data.json', result_as='result', mode='read')
file_task.method()
print(file_task.data)  # Output: Content of the file
```
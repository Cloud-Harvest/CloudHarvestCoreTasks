# FileTask([BaseTask](./base_task)) | `file`
This task performs various file operations, such as reading and writing data. 

- [Configuration](#configuration)
    - [Directives](#directives)
- [Examples](#example)

## Configuration

### Directives

| Key          | Required | Default | Description                                                                                                                                                                                                                                                    |
|--------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path         | Yes      | None    | The path to the file.                                                                                                                                                                                                                                          |
| mode         | Yes      | None    | The mode in which the file will be opened (`append`, `read`, `write`).                                                                                                                                                                                         |
| format       | No       | None    | The format of the file (`config`, `csv`, `json`, `raw`, `yaml`). When not provided, the FileTask attempts to determine the correct format based on the `path` extension. If the extension cannot be determined or is an unsupported type, the format is `raw`. |
| desired_keys | No       | None    | When specified, only the keys identified will be written to the file.                                                                                                                                                                                          |
| template     | No       | None    | A `jinja2` template to use when writing data.                                                                                                                                                                                                                  |

## Example

### Read example
```yaml
file:
  name: Read File
  description: | 
    This task will read a file from /tmp/my_source_file.json
    Because the extension is .json, the format will be set to json and does not need to be set with the 'format' directive.
  path: /tmp/my_source_file.json
  mode: read
```

### Write example
```yaml
file:
  name: Write File
  description: | 
    This task will write a file to /tmp/my_destination_file.json
    Because the extension is .json, the format will be set to json and does not need to be set with the 'format' directive.
  data: var.my_data
  desired_keys:   # Only these keys will be written to the file
    - key1
    - nested.key.key2
  path: /tmp/my_destination_file.json
  mode: write
```


# JsonTask([BaseTask](./base_task.md)) | `json`
The JsonTask performs serialization and deserialization of JSON operations. This allows users to convert JSON to strings
and vice versa, respectively.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

## Configuration

### Directives

| Field           | Default | Required | Description                                                                                                                                                                                                               |
|-----------------|---------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| mode            | None    | Yes      | Must be `serialize` or `deserialize`. The mode in which the JSON operation will be performed. Serlization converts a Python object into a JSON string, while deserialization converts a JSON string into a Python object. |
| data            | None    | Yes      | The data to be serialized or deserialized. The data can be a Python object or a JSON string.                                                                                                                              |
| default_type    | `str`   | No       | The default type to use when deserializing JSON. If the JSON string cannot be deserialized into the specified type, the default type will be used instead.                                                                |
| parse_datetimes | `False` | No       | If `True`, the JSON string will be parsed for datetime objects. If the string cannot be parsed, it is returned as-is. If `False`, the JSON string will be treated as a string.                                            |

# Example
```yaml
json:
  name: Serialize data
  description: Converts a Python object into a JSON string
  result_as: my_serialized_data
  mode: serialize
  data: var.data
```

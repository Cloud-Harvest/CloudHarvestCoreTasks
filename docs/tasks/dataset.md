# DataSetTask([BaseTask](./base_task.md)) | `dataset`
The `DataSetTask` class is a subclass of the `BaseTask` class. It represents a task that modifies a record set by 
applying functions to its records.

In a Task Chain, you called this task by using the key `dataset`.

* [Configuration](#configuration)
  * [Directives](#directives) 
    * [Stages](#stages)
      * [add_keys](#add_keys)
      * [add_records](#add_records)
      * [append_record_maths_results](#append_record_maths_results)
      * [cast_key](#cast_key)
      * [convert_list_of_dict_to_dict](#convert_list_of_dict_to_dict)
      * [convert_list_to_string](#convert_list_to_string)
      * [convert_string_to_list](#convert_string_to_list)
      * [copy_key](#copy_key)
      * [copy_record](#copy_record)
      * [create_key_from_keys](#create_key_from_keys)
      * [deserialize_key](#deserialize_key)
      * [drop_keys](#drop_keys)
      * [flatten](#flatten)
      * [limit](#limit)
      * [match_and_remove](#match_and_remove)
      * [maths_keys](#maths_keys)
      * [maths_records](#maths_records)
      * [maths_reset](#maths_reset)
      * [nest_keys](#nest_keys)
      * [remove_duplicate_records](#remove_duplicate_records)
      * [remove_duplicates_from_list](#remove_duplicates_from_list)
      * [rename_keys](#rename_keys)
      * [serialize_key](#serialize_key)
      * [set_keys](#set_keys)
      * [slice_records](#slice_records)
      * [sort_records](#sort_records)
      * [splice_key](#splice_key)
      * [split_key](#split_key)
      * [title_keys](#title_keys)
      * [unflatten](#unflatten)
      * [unwind](#unwind)
      * [wind](#wind)
* [Example](#example)

## Configuration

### Directives
The `DataSetTask` class has the following arguments beyond those defined in [BaseTask](./base_task).

| Directive | Required | Default | Description                                                                                                  |
|-----------|----------|---------|--------------------------------------------------------------------------------------------------------------|
| stages    | Yes      | None    | A list of dictionaries containing the function name and arguments to be applied to the dataset sequentially. |

### Stages
The following sections describe the directives available for each stage in the `stages` list. As a general rule, stages
whose names end in `_key` or `_keys` operate on a per-record basis, while stages whose names end in `_records` operate
across the entire dataset.

#### `add_keys`
The `add_keys` stage adds new keys to the records in the dataset.

| Directive       | Required | Default | Description                                                                          |
|-----------------|----------|---------|--------------------------------------------------------------------------------------|
| `keys`          | Yes      |         | A list of dictionaries containing the key name and value to be added to the records. |
| `default_value` | No       | `None`  | The default value to be used if the key is not present in the record.                |

```yaml
stages:
  - add_keys:
    keys:
      - new_key1
      - new_key2
    default_value: my_default_value
```

#### `add_records`
Extends a dataset with new records.

| Directive | Required | Default | Description                                                                                                                                  |
|-----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `records` | Yes      |         | A dictionary or list of dictionaries to be added to the dataset. Also accepts `var.` which resolves to a dictionary or list of dictionaries. |

```yaml
stages:
  - add_records:
    records:
      - key1: value1
        key2: value2
      - key1: value3
        key2: value4
```

#### `append_record_maths_results`
Appends the results of mathematical operations to the records in the dataset.

| Directive                 | Required | Default  | Description                                            |
|---------------------------|----------|----------|--------------------------------------------------------|
| `record_identifier_key`   | No       | `_id`    | Indicates the record identifying field in the DataSet. |
| `record_identifier_value` | No       | `Totals` | The value to place in the `record_identifier_key`.     |

```yaml
stages:
  - append_record_maths_results:
    record_identifier_key: Name
    record_identifier_value: Sum
```

#### `cast_key`
Converts a key from one data type to another. If type conversion is not possible, the key's value will change to `None`.

| Directive     | Required | Default | Description                                                                                               |
|---------------|----------|---------|-----------------------------------------------------------------------------------------------------------|
| `source_key`  | Yes      |         | The key to be converted.                                                                                  |
| `target_type` | Yes      |         | One of `bool`, `str`, `int`, `float`, `list`, `dict`, `datetime.fromtimestamp`, `datetime.fromisoformat`  |
| `target_key`  | No       |         | The target data type. If not provided, the `source_key` will be overwritten.                              | 

```yaml
stages:
  - cast_key:
      source_key: my_key
      target_type: int
```

#### `convert_list_of_dict_to_dict`
Converts a list of dictionaries to a dictionary. The key in the dictionaries to use as the key in the resulting 
dictionary is specified by the `name_key` argument. The key in the dictionaries to use as the value in the resulting 
dictionary is specified by the `value_key` argument.

| Directive    | Required | Default | Description                                                                  |
|--------------|----------|---------|------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the list of dictionaries to be converted to a dictionary. |
| `target_key` | Yes      |         | The key to store the resulting dictionary.                                   |
| `name_key`   | No       | `Name`  | The key in the dictionaries to use as the key in the resulting dictionary.   |
| `value_key`  | No       | `Value` | The key in the dictionaries to use as the value in the resulting dictionary. |

**Data**
```json
[
  {
    "Name": "key1",
    "Value": "value1"
  },
  {
    "Name": "key2",
    "Value": "value2"
  }
]
```

**Config**
```yaml
stages:
  - convert_list_of_dict_to_dict:
      source_key: my_key
      target_key: my_new_key
      name_key: my_name_key
      value_key: my_value_key
```

**Yields**
```json
{
  "key1": "value1",
  "key2": "value2"
}
```

#### `convert_list_to_string`
Converts a list of items into a string.

| Directive    | Required | Default | Description                                                                                   |
|--------------|----------|---------|-----------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the list of items to be converted to a string.                             |
| `target_key` | No       |         | The key to store the resulting string. If not provided, the `source_key` will be overwritten. |
| `separator`  | No       | `\n`    | The separator to use when joining the list items. By default, the separator is a newline.     |

```yaml
stages:
  - convert_list_to_string:
      source_key: my_key
      target_key: my_new_key
      separator: ','
```

#### `convert_string_to_list`
Converts a string into a list of items based on a separator.

| Directive    | Required | Default | Description                                                                                 |
|--------------|----------|---------|---------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the string to be converted to a list.                                    |
| `target_key` | No       |         | The key to store the resulting list. If not provided, the `source_key` will be overwritten. |
| `separator`  | No       | `\n`    | The separator to use when splitting the string. By default, the separator is a new line.    |

```yaml
stages:
  - convert_string_to_list:
      source_key: my_key
      target_key: my_new_key
      separator: ','
```

#### `copy_key`
Copies the value of one key to another key.

| Directive    | Required | Default | Description                     |
|--------------|----------|---------|---------------------------------|
| `source_key` | Yes      |         | The key to copy the value from. |
| `target_key` | Yes      |         | The key to copy the value to.   |

```yaml
stages:
  - copy_key:
      source_key: my_key
      target_key: my_new_key
```

#### `copy_record`
Duplicates a record in the dataset.

| Directive      | Required | Default | Description                                                                                    |
|----------------|----------|---------|------------------------------------------------------------------------------------------------|
| `source_index` | Yes      |         | The index of the record to be copied.                                                          |
| `target_index` | No       |         | The index to insert the copied record. If not provided, the record is appended to the dataset. |

```yaml
stages:
  - copy_record:
      source_index: 0
      target_index: 1
```

#### `create_key_from_keys`
Creates a new key by concatenating the values of other keys.

| Directive     | Required | Default | Description                                                                                            |
|---------------|----------|---------|--------------------------------------------------------------------------------------------------------|
| `source_keys` | Yes      |         | A list of keys to concatenate.                                                                         |
| `target_key`  | No       |         | The key to store the resulting string. If not provided, the `source_keys` will be overwritten.         |
| `separator`   | No       | ` `     | The separator to use when joining the values of the source keys. By default, the separator is a space. |

```yaml
stages:
  - create_key_from_keys:
      source_keys:
        - key1
        - key2
      target_key: new_key
      separator: ','
```

#### `deserialize_key`
Deserializes a JSON string into a dictionary, converting it from a string to Python structure.

| Directive    | Required | Default | Description                                                                                       |
|--------------|----------|---------|---------------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the JSON string to be deserialized.                                            |
| `target_key` | No       |         | The key to store the resulting dictionary. If not provided, the `source_key` will be overwritten. |

```yaml
stages:
  - deserialize_key:
      source_key: my_key
      target_key: my_new_key
```

#### `drop_keys`
Removes keys from the records in the dataset.

| Directive | Required | Default | Description                                    |
|-----------|----------|---------|------------------------------------------------|
| `keys`    | Yes      |         | A list of keys to be removed from the records. |

```yaml
stages:
  - drop_keys:
      keys:
        - key1
        - key2
```

#### `flatten`
Flattens the DataSet by converting each record's nested dictionaries into top-level keys.

| Directive        | Required | Default | Description                                      |
|------------------|----------|---------|--------------------------------------------------|
| `preserve_lists` | No       | `False` | If `True`, lists will not be flattened.          |
| `separator`      | No       | `.`     | The separator to use when creating the new keys. |

```yaml
stages:
  - flatten:
      preserve_lists: True
      separator: '_'
```

#### `limit`
Limits the number of records in the dataset, discarding records beyond the specified value.

| Directive | Required | Default | Description                                |
|-----------|----------|---------|--------------------------------------------|
| `limit`   | Yes      |         | The maximum number of records to retain.   |

```yaml
stages:
  - limit:
      limit: 10
```

#### `match_and_remove`
Evaluates records based on matching criteria. Records which do not match the criteria are removed from the dataset.
See [filtering documentation](../filters.md) for more information on matching.

| Directive           | Required | Default | Description                                                       |
|---------------------|----------|---------|-------------------------------------------------------------------|
| `matching_criteria` | Yes      |         | List if lists containing strings used to evaluate the expression. |
| `invert_results`    | No       | `False` | If `True`, records that match the criteria are removed.           |

Each string between the brackets of a `matching_criteria` is part of an AND expression while each set of brackets is part of an OR expression.
Therefore, the following expression can be read as:
> Remove records where key1 equals value1 `AND` key2 is greater than value2 `OR` key3 does not equal value3.

```yaml
stages:
- match_and_remove:
    matching_criteria:
        - ['key1==value1', 'key2>value2']   
        - ['key3!=value3']
```

#### `maths_keys`
Performs mathematical operations on the values of keys in the records. 
This method is distinct from [`maths_records`](#maths_records) in that it operations on a per-record basis, rather than 
across the entire dataset.

| Directive       | Required | Default | Description                                                                                                                     |
|-----------------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------|
| `source_keys`   | Yes      |         | A list of keys to perform the mathematical operation on.                                                                        |
| `target_key`    | Yes      |         | The key to store the result of the mathematical operation. If not provided, the `source_keys` will be overwritten.              |
| `operation`     | Yes      |         | The mathematical operation to perform. Must be one of `add`, `subtract`, `multiply`, `divide`, `average`, `minimum`, `maximum`. |
| `missing_value` | Yes      |         | The value to use when a key is missing from a record.                                                                           |
| `default_value` | No       | `None`  | The value to use when a key's result is `None`.                                                                                 |

```yaml
stages:
  - maths_keys:
      source_keys:
        - key1
        - key2
      target_key: new_key
      operation: add
      missing_value: 0
      default_value: 0
```

#### `maths_records`
Performs mathematical operations on the values of keys in the records.
This method is distinct from [`maths_keys`](#maths_keys) in that it operations across the entire dataset, rather than on a per-record basis.

> **Note:** To populate the results of a `maths_records` operation in the final dataset output, it is necessary to call 
> the [`append_record_maths_results`](#append_record_maths_results) stage when all mathematical operations are complete.

| Directive       | Required | Default | Description                                                                                                                                                                                                                                                                         |
|-----------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `source_key`    | Yes      |         | The key to perform the mathematical operation on.                                                                                                                                                                                                                                   |
| `target_key`    | Yes      |         | The key to assign the result to in the `DataSet.maths_results` attribute. The `target_key` may be the same as the `source_key` and it will not override them. Instead, the `append_record_maths_results` stage will add them to the end of the dataset as a new record when called. |
| `operation`     | Yes      |         | The mathematical operation to perform. Must be one of `add`, `subtract`, `multiply`, `divide`, `average`, `minimum`, `maximum`.                                                                                                                                                     |
| `missing_value` | Yes      |         | The value to use when a key is missing from a record.                                                                                                                                                                                                                               |
| `default_value` | No       | `None`  | The value to use when a key's result is `None`.                                                                                                                                                                                                                                     |

```yaml
stages:
  - maths_records:
      source_key: my_key
      target_key: my_new_key
      operation: add
      missing_value: 0
      default_value: 0
```

#### `maths_reset`
Resets the `DataSet.maths_results` attribute to an empty dictionary, allowing new mathematical operations to be performed and stored.

> This method has no directives.

```yaml
stages:
  - maths_reset:
```

#### `nest_keys`
Places values from a record under a new key in the record. If no `target_key` is provided, the source keys are placed at the top level of the record, essentially un-nesting them.

| Directive                | Required | Default | Description                                                                                                                                  |
|--------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `source_keys`            | Yes      |         | A list of keys to nest under a new key.                                                                                                      |
| `target_key`             | No       |         | The key to store the nested keys under.                                                                                                      |
| `preserve_original_keys` | No       | `False` | If `True`, the original keys will be preserved in the record. Otherwise, the keys defined in `source_keys` will be removed from the dataset. |

```yaml
stages:
  - nest_records:
      source_keys:
          - key1
          - key2
      target_key: new_key
      preserve_original_keys: True
```

#### `remove_duplicate_records`
Removes duplicate records from the dataset. A record is considered "duplicate" if, when converted to a string, it is
identical to another record in the dataset.

> This method has no directives.

```yaml
stages:
  - remove_duplicate_records:
```

#### `remove_duplicates_from_list`
Removes duplicate items from a list.

| Directive    | Required | Default | Description                                                                                 |
|--------------|----------|---------|---------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the list of items to remove duplicates from.                             |
| `target_key` | No       |         | The key to store the resulting list. If not provided, the `source_key` will be overwritten. |

```yaml
stages:
  - remove_duplicates_from_list:
      source_key: my_key
      target_key: my_new_key
```

#### `rename_keys`
Renames keys in the records in the dataset.

| Directive    | Required | Default | Description                                                                                   |
|--------------|----------|---------|-----------------------------------------------------------------------------------------------|
| `key_map`    | Yes      |         | A dictionary containing the old key names as keys and the new key names as values.            |

```yaml
stages:
  - rename_keys:
      key_map:
          old_key1: new_key1
          old_key2: new_key2
```

#### `serialize_key`
Serializes a dictionary into a JSON string, converting it from a Python structure to a string.

| Directive    | Required | Default | Description                                                                                        |
|--------------|----------|---------|----------------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the dictionary to be serialized.                                                |
| `target_key` | No       |         | The key to store the resulting JSON string. If not provided, the `source_key` will be overwritten. |

```yaml
stages:
  - serialize_key:
      source_key: my_key
      target_key: my_new_key
```

#### `set_keys`
Sets the value of keys in the records in the dataset, removing any key that is not explicitly provided.

| Directive | Required | Default | Description                                 |
|-----------|----------|---------|---------------------------------------------|
| `keys`    | Yes      |         | A list containing the key names to be kept. |

```yaml
stages:
  - set_keys:
      keys:
        - key1
        - key2
```

#### `slice_records`
Only retains records within the start and end index range. This method follows the Python list slicing convention. For
example, `start_index = 3` and `end_index = -1` will retain records 3 through the second-to-last record.

| Directive     | Required | Default | Description                                                                                        |
|---------------|----------|---------|----------------------------------------------------------------------------------------------------|
| `start_index` | No       |         | The index of the first record to remove.                                                           |
| `end_index`   | No       |         | The index of the last record to remove. If not provided, only the `start_index` record is removed. |

```yaml
stages:
  - subset_records:
      start_index: 0
      end_index: 10
```

#### `sort_records`
Sorts the records in the dataset based on the keys and directions provided.

| Directive | Required | Default | Description                                                                           |
|-----------|----------|---------|---------------------------------------------------------------------------------------|
| `keys`    | Yes      |         | A list of strings containing the key name and optionally the direction to sort it by. |

```yaml
stages:
  - sort_records:
      keys:
        - key1        # Sort by key1 in the default ascending order.
        - key2:desc   # Sort by key2 in descending order.
```

#### `splice_key`
Extracts a substring from a key's value.

| Directive    | Required | Default | Description                                                                                      |
|--------------|----------|---------|--------------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the string to extract a substring from.                                       |
| `target_key` | No       |         | The key to store the resulting substring. If not provided, the `source_key` will be overwritten. |
| `start`      | No       |         | The starting index of the substring.                                                             |
| `end`        | No       |         | The ending index of the substring.                                                               |
| `step`       | No       |         | The step size to use when extracting the substring.                                              |

```yaml
stages:
  - splice_key:
      source_key: my_key
      target_key: my_new_key
      start: 0
      end: 10
      step: 2
```

#### `split_key`
Splits a string into a list of substrings based on a separator.

| Directive    | Required | Default | Description                                                                                 |
|--------------|----------|---------|---------------------------------------------------------------------------------------------|
| `source_key` | Yes      |         | The key containing the string to split.                                                     |
| `target_key` | No       |         | The key to store the resulting list. If not provided, the `source_key` will be overwritten. |
| `separator`  | No       |         | The separator to use when splitting the string. By default, the separator is a new line.    |

```yaml
stages:
  - split_key:
      source_key: my_key
      target_key: my_new_key
      separator: ','
```

#### `title_keys`
Uses the Python string method `title()` to capitalize the first letter of each word in a key's value.

| Directive               | Required | Default | Description                                                                              |
|-------------------------|----------|---------|------------------------------------------------------------------------------------------|
| `remove_characters`     | No       |         | A list of characters to remove from the key's value after capitalizing the first letter. |
| `replacement_character` | No       |         | The character to replace the characters in `remove_characters` with.                     |

```yaml
stages:
  - title_keys:
      remove_characters:
        - ','
        - '.'
      replacement_character: '_'
```

#### `unflatten`
Reverses a previous [`flattened`](#flatten) stage.

| Directive   | Required | Default | Description                                      |
|-------------|----------|---------|--------------------------------------------------|
| `separator` | No       | `.`     | The separator to use when creating the new keys. |

```yaml
stages:
  - unflatten:
      separator: '_'
```

#### `unwind`
Creates new records for each item in a list, duplicating the original record for each item.

| Directive                      | Required | Default | Description                                                                                                    |
|--------------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------|
| `source_key`                   | Yes      |         | The key containing the list of items to create new records from.                                               |
| `preserve_null_and_empty_keys` | No       | False   | If `True`, keys with `None` or empty values will be preserved in the new records. Otherwise, they are skipped. |

```yaml
stages:
  - unwind:
      source_key: my_key
      preserve_null_and_empty_keys: True
```

#### `wind`
The `wind` stage is the inverse of the [`unwind`](#unwind) stage, converting each item into a list of items.

| Directive                        | Required | Default | Description                                                                                                     |
|----------------------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| `source_key`                     | Yes      |         | The key containing the list of items to convert into a single list.                                             |
| `preserve_null_and_empty_values` | No       |         | If `True`, keys with `None` or empty values will be preserved in the new records. Otherwise, they are skipped.  |

```yaml
stages:
  - wind:
      source_key: my_key
```

### Example

```yaml
name: DataSet Task
description: A task that modifies a record set by applying functions to its records.
data: my_dataset
result_as: new_dataset
stages:
  - unwind: 
      source_key: my_key
      preserve_null_and_empty_keys: True
  - rename_keys:
      key_map:
        old_key1: new_key1
        old_key2: new_key2
  - match_and_remove:
      matching_criteria:
          - ['key1==value1', 'key2>value2']   
          - ['key3!=value3']
```

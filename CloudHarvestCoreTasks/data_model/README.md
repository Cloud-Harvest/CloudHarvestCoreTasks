# Harvest Core Data Model
The CDM is responsible for converting parameters into data manipulation instructions the API, CLI, or Data Collectors can interpret.

## Table of Contents
- [Harvest Core Data Model](#harvest-core-data-model)
- [Usage](#usage)
  - [Harvest Record Set](#harvest-record-set)
    - [add](#add)
    - [add_match](#add_match)
    - [clear_matches](#clear_matches)
    - [create_index](#create_index)
    - [drop_index](#drop_index)
    - [rebuild_indexes](#rebuild_indexes)
    - [remove_duplicates](#remove_duplicates)
    - [remove_unmatched_records](#remove_unmatched_records)
    - [unwind](#unwind)
  - [Harvest Record](#harvest-record)
    - [add_key_from_keys](#add_key_from_keys)
    - [assign_elements_at_index_to_key](#assign_elements_at_index_to_key)
    - [cast](#cast)
    - [clear_matches](#clear_matches)
    - [copy_key](#copy_key)
    - [dict_from_json_string](#dict_from_json_string)
    - [first_not_null_value](#first_not_null_value)
    - [flatten](#flatten)
    - [key_value_list_to_dict](#key_value_list_to_dict)
    - [list_to_str](#list_to_str)
    - [match](#match)
    - [remove_key](#remove_key)
    - [rename_key](#rename_key)
    - [reset_matches](#reset_matches)
    - [split_key](#split_key)
    - [substring](#substring)
    - [unflatten](#unflatten)
- [License](#license)

# Usage


## Harvest Record Set Task
The Harvest Record Set Task is a task that can be used to manipulate a record set. 
It is a subclass of the `BaseTask` class and can be used in a `TaskChain`.

### Parameters
| Parameter      | Description                                                        |
|----------------|--------------------------------------------------------------------|
| stages         | A list of methods and their arguments to perform on the record set |

### Example
```yaml
tasks:
  - recordset:
      name: Update Tags
      description: Update the tags to use a dict format
      data: clusters
      results_as: results
      stages:
        - key_value_list_to_dict:
            source_key: TagList
            name_key: Name
            value_key: Value
            target_key: Tags
            preserve_original: False
      
# Original Recordset Data: [{'TagList': [{'Name': 'tag1', 'Value': 'value1'}, {'Name': 'tag2', 'Value': 'value2'}]}]
# Output Recordset Data: [{'Tags': {'tag1': 'value1', 'tag2': 'value2'}}]
```

## Harvest Record Set
### add
This method adds a list of records to the record set. It accepts a list of dictionaries or 
HarvestRecord objects, a single dictionary, or a single HarvestRecord object. After adding the 
new data, the indexes of the record set are rebuilt.

#### Parameters

| Parameter | Description                                                                                                                                                     |
|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| data      | A list of dictionaries or HarvestRecord objects to add to the record set. If the object is a dictionary, it will automatically be converted to a HarvestRecord. |

#### Example
```yaml
add:
  data: [{'field1': 'value1', 'field2': 'value2'}, {'field1': 'value3', 'field2': 'value4'}]

# Starting Data: [{'field1': 'value1', 'field2': 'value2'}]
# Input: [{'field1': 'value3', 'field2': 'value4'}]
# Output: [{'field1': 'value1', 'field2': 'value2'}, {'field1': 'value3', 'field2': 'value4'}]
```

### add_match
This method adds a match to the record set.

#### Parameters

| Parameter | Description             |
|-----------|-------------------------|
| syntax    | The match syntax to add |

#### Example
```yaml
add_match:
  syntax: "field1=value1"
```

### clear_matches
This method clears all matches from the record set. It has no parameters and can be entered as below.

#### Example
```yaml
clear_matches:
```

### create_index
This method creates an index for the record set. Indexes are required to join recordsets.

#### Parameters

| Parameter  | Description                                  |
|------------|----------------------------------------------|
| index_name | The name of the index                        |
| fields     | A list of the fields to include in the index |

#### Example
```yaml
create_index:
  index_name: "index1"
  fields: 
    - field1
    - field2
```

### drop_index
This method drops an index from the record set.

#### Parameters

| Parameter  | Description                   |
|------------|-------------------------------|
| index_name | The name of the index to drop |

#### Example
```yaml
drop_index:
  index_name: "index1"
```

### rebuild_indexes
This method rebuilds all indexes for the record set. Useful when an indexed field has been modified. This method
has no parameters.

#### Example
```yaml
rebuild_indexes:
```

### remove_duplicates
This method removes duplicate records from the record set.

#### Example
```yaml
remove_duplicates:

# Input: [{'field1': 'value1', 'field2': 'value2'}, {'field1': 'value1', 'field2': 'value2'}]
# Output: [{'field1': 'value1', 'field2': 'value2'}]
```

### remove_unmatched_records
This method removes all records in the record set that are not a match. To use this method, perform a an 
[`add_match`](#add_match) operation first. This method has no parameters.

#### Example
```yaml
remove_unmatched_records:
```

### unwind
This method unwinds a list of records in the record set into separate records. The key of the list to unwind
must be a `list` of values. 

#### Parameters

| Parameter                    | Description                                                          |
|------------------------------|----------------------------------------------------------------------|
| source_key                   | The key of the list to unwind                                        |
| preserve_null_and_empty_keys | Whether to preserve keys with null or empty values, defaults to True |

#### Example

```yaml
unwind:
  source_key: "field1"
  preserve_null_and_empty_keys: True

# Input: [{'field1': ['value1', 'value2'], 'field2': 'value3'}]
# Output: [{'field1': 'value1', 'field2': 'value3'}, {'field1': 'value2', 'field2': 'value3'}]
```

## Harvest Record

### add_key_from_keys
This method creates a new key in the record, with its value being a concatenation of the values
of the keys provided in the sequence. If a value in the sequence is not a key, it is interpreted
as a literal string.

#### Parameters

| Parameter     | Description                                                                          |
|---------------|--------------------------------------------------------------------------------------|
| new_key       | The name of the new key to be added to the record                                    |
| sequence      | A list of keys whose values will be concatenated to form the value of the new key    |
| delimiter     | The delimiter to use when concatenating the values. Defaults to a space character    |
| abort_on_none | If True, the method will abort if a value in the sequence is None. Defaults to False |

#### Example
```yaml
add_key_from_keys:
  new_key: "newKey"
  sequence: ["key1", "key2"]
  delimiter: " "
  abort_on_none: False

# Input: {"key1": "value1", "key2": "value2"}
# Output: {"newKey": "value1 value2"}
```

### assign_elements_at_index_to_key
Assign elements at a specific index to a new key.

#### Parameters

| Parameter  | Description                                                      |
|------------|------------------------------------------------------------------|
| source_key | The name of the source key                                       |
| target_key | The name of the target key                                       |
| start      | The index start position                                         |
| end        | The index end position                                           |
| delimiter  | The delimiter to use when joining the elements, defaults to None |

#### Example
```yaml
assign_elements_at_index_to_key:
  source_key: "sourceKey"
  target_key: "targetKey"
  start: 0
  end: 2
  delimiter: ","

# Input: {"sourceKey": ["value1", "value2", "value3"]}
# Output: {"targetKey": "value1,value2"}
```

### cast
Cast the value of a key to a different type.

#### Parameters

| Parameter     | Description                                                                                                           |
|---------------|-----------------------------------------------------------------------------------------------------------------------|
| source_key    | The name of the key                                                                                                   |
| format_string | The type to cast the value to                                                                                         |
| target_key    | When provided, a new key will be created with the cast value, defaults to None which overrides the existing key value |

#### Example
```yaml
cast:
  source_key: "sourceKey"
  format_string: "int"
  target_key: "targetKey"

# Output: {"sourceKey": "123", "targetKey": 123}
```

### clear_matches
Removes all matches from the recordset provided by `add_match`. 

#### Example
```yaml
clear_matches:
```

### copy_key
Copy the value of a key to a new key.

#### Parameters

| Parameter  | Description                |
|------------|----------------------------|
| source_key | The name of the source key |
| target_key | The name of the target key |

#### Example
```yaml
copy_key:
  source_key: "sourceKey"
  target_key: "targetKey"

# Output: {"sourceKey": "value1", "targetKey": "value1"}
```

### dict_from_json_string
Convert a JSON string to a dictionary.
When 'key', the JSON string is converted to a dictionary and stored in a new key.
When 'merge', the JSON string is converted to a dictionary and merged with the existing record.
When 'replace', the JSON string is converted to a dictionary and replaces the existing record.

#### Parameters

| Parameter  | Description                                             |
|------------|---------------------------------------------------------|
| source_key | The name of the key containing the JSON string          |
| operation  | The operation to perform ('key', 'merge', or 'replace') |
| new_key    | The name of the new key, defaults to None               |

| Operation | Description                                                                                 |
|-----------|---------------------------------------------------------------------------------------------|
| 'key'     | The new dictionary is stored in the existing recordset with the value provided in `new_key` |
| 'merge'   | The new dictionary is merged with the existing record, overriding any existing keys         |
| 'replace' | The new dictionary replaces the entire existing record                                      |

#### Example
```yaml
dict_from_json_string:
  source_key: "sourceKey"
  operation: "merge"
  new_key: "newKey"

# Input: {"sourceKey": "{\"key1\": \"value1\"}"}
# Output (key) : {"sourceKey": "{\"key1\": \"value1\"}", "newKey": {"key1": "value1"}}
# Output (merge) : {"sourceKey": "{\"key1\": \"value1\"}", "key1": "value1"}
# Output (replace) : {"key1": "value1"}
```

### first_not_null_value
Get the first non-null value among a list of keys.

#### Parameters

| Parameter | Description       |
|-----------|-------------------|
| keys      | The keys to check |

#### Example
```yaml
first_not_null_value:
  keys: ["key1", "key2", "key3"]

# Input: {"key1": None, "key2": "value2", "key3": "value3"}
# Output: "value2"
```

### flatten
Flatten the record.

#### Parameters

| Parameter | Description                                           |
|-----------|-------------------------------------------------------|
| separator | The separator to use when flattening, defaults to '.' |

#### Example
```yaml
flatten:
  separator: "."

# Input: {"key1": {"key2": "value1"}}
# Output: {"key1.key2": "value1"}
```

### key_value_list_to_dict
Convert a list of key-value pairs to a dictionary.

#### Parameters

| Parameter         | Description                                                        |
|-------------------|--------------------------------------------------------------------|
| source_key        | The name of the source key                                         |
| target_key        | When provided, the result is placed in a new key, defaults to None |
| name_key          | The name of the key in the source list, defaults to 'Key'          |
| value_key         | The name of the value in the source list, defaults to 'Value'      |
| preserve_original | Whether to preserve the original key, defaults to False            |

#### Example
```yaml
key_value_list_to_dict:
  source_key: "sourceKey"
  target_key: "targetKey"
  name_key: "Key"
  value_key: "Value"
  preserve_original: False

# Input: {"sourceKey": [{"Key": "key1", "Value": "value1"}, {"Key": "key2", "Value": "value2"}]}
# Output: {"targetKey": {"key1": "value1", "key2": "value2"}}
```

### list_to_str
Convert a list to a string.

#### Parameters

| Parameter  | Description                                                                |
|------------|----------------------------------------------------------------------------|
| source_key | The name of the source key                                                 |
| target_key | The name of the target key, defaults to None                               |
| delimiter  | The delimiter to use when joining the elements, defaults to '\n' (newline) |

#### Example
```yaml
list_to_str:
  source_key: "sourceKey"
  target_key: "targetKey"
  delimiter: ","

# Input: {"sourceKey": ["value1", "value2", "value3"]}
# Output: {"targetKey": "value1,value2,value3"}
```

### match
Check if the record matches a statement.

#### Parameters

| Parameter | Description         |
|-----------|---------------------|
| syntax    | The match statement |

#### Example
```yaml
match:
  syntax: "field1=value1"
```

### remove_key
Remove a key from the record.

#### Parameters

| Parameter | Description                   |
|-----------|-------------------------------|
| key       | The name of the key to remove |

#### Example
```yaml
remove_key:
  key: "keyToRemove"

# Input: {"keyToRemove": "value1", "keyToKeep": "value2"}
# Output: {"keyToKeep": "value2"}
```

### rename_key
Rename a key in the record.

#### Parameters

| Parameter | Description             |
|-----------|-------------------------|
| old_key   | The name of the old key |
| new_key   | The name of the new key |

#### Example
```yaml
rename_key:
  old_key: "oldKey"
  new_key: "newKey"

# Input: {"oldKey": "value1"}
# Output: {"newKey": "value1"}
```

### reset_matches
Reset the matches of the record.

#### Example
```yaml
reset_matches:
```

### split_key
Split the value of a key into a list.

#### Parameters

| Parameter  | Description                                          |
|------------|------------------------------------------------------|
| source_key | The name of the source key                           |
| target_key | The name of the target key, defaults to None         |
| delimiter  | The delimiter to use when splitting, defaults to ' ' |

#### Example
```yaml
split_key:
  source_key: "sourceKey"
  target_key: "targetKey"
  delimiter: ","

# Input: {"sourceKey": "value1,value2,value3"}
# Output: {"targetKey": ["value1", "value2", "value3"]}
```

### substring
Get a substring of the value of a key. Follows the indices of a Python slice.

#### Parameters

| Parameter  | Description                                                        |
|------------|--------------------------------------------------------------------|
| source_key | The name of the source key                                         |
| start      | The start index of the substring                                   |
| end        | The end index of the substring                                     |
| target_key | When provided, the result is placed in a new key, defaults to None |

#### Example
```yaml
substring:
  source_key: "sourceKey"
  start: 0
  end: 5
  target_key: "targetKey"

# Input: {"sourceKey": "value1value2"}
# Output: {"sourceKey": "value1value2", "targetKey": "value1"}
```

### unflatten
Unflatten the record.

#### Parameters

| Parameter | Description                                             |
|-----------|---------------------------------------------------------|
| separator | The separator to use when unflattening, defaults to '.' |

#### Example
```yaml
unflatten:
  separator: "."

# Input: {"key1.key2": "value1"}
# Output: {"key1": {"key2": "value1"}}
```

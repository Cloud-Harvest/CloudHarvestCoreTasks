# RedisTask([BaseDataTask](./base_data.md)) | `redis`
This task connects to a Redis database and performs some operation.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

## Configuration

### Directives

| Directive        | Required  | Default | Description                                                                                                                                                  |
|------------------|-----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `command`        | Yes       | None    | Any Redis client command. Commands can be provided in any case; they will be converted to lower case at runtime.                                             |
| `rekey`          | No        | False   | Certain commands, such as `hget`, will only return a record's values but not the keys. When true, keys are added back to the result.                         |
| `serializer`     | No        | None    | One of `hset`, `hget`, `serialize`, `deserialize`. Formats data when reading/writing from/to Redis. See subsection on serializers for more information.      |
| `serializer_key` | Sometimes | None    | Required `serializer` is set to `hset` or `serialize`. Indicates what argument field the serialization method will act upon (typically `mapping` or `value`) |


### Serializers

| Serializer    | When to use                                                      | Description                                                                                                                            |
|---------------|------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `hset`        | When writing with Redis Hashes (e.g., `HSET`, `HMSET`)           | Serializes the data into a Redis hash, leaving the top-level keys in place but ensuring all values are properly encoded Redis objects. |
| `hget`        | When reading data serialized with `hset` (e.g., `HGET`, `HMGET`) | Deserializes the data from a Redis hash, converting results from a previous `hset` serialization operation.                            |
| `serialize`   | When writing an entire object as a single string (e.g., `SET`)   | Serializes the entire object to be written into a string.                                                                              |
| `deserialize` | When reading an object serialized as a string (e.g., `GET`)      | Deserializes the entire object from a string.                                                                                          |

> Most internal Harvest operations will use either `HSET`, `HGET`, or `HGETALL` because these operations allow us to 
> read or manipulate the data in a more granular way, thus reducing operational overhead. However, you can use `SET` 
> and `GET` as necessary.

## Example
In the following example, we will demonstrate how to `SCAN` for matching records based on a pattern, then retrieve the
actual record content for each matching record using `HSET`. This is likely to be one of the most common scenarios when
working with Redis and Harvest.

```yaml
tasks:
  - redis:
      name: My Redis Scan Task
      description: Scan Redis for matching keys
      command: scan
      arguments:
        pattern: "some*pattern*"
        count: 100
      result_as: matching_redis_names
      result_to_list_with_key: redis_name   # Converts the list to a list of dictionaries with the key `redis_name`
  
  - redis:
      name: My Redis Task
      command: hget
      arguments:
        name: item.redis_name
        serializer: hget
        serializer_key: my_redis_key
      iterate: var.matching_redis_names     # Iterates over the earlier list of matching Redis names
      result_as:
        name: result                        # Resulting task variable name
        mode: append                        # Append the result of each iteration to the variable
        include:                            # A dictionary of key/value pairs to include in the result (or each item of the result)
          redis_name: item.redis_name
```
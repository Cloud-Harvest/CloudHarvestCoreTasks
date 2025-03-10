# RedisTask([BaseDataTask](./base_data.md)) | `redis`
This task connects to a Redis database and performs some operation.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

## Configuration

### Directives

| Directive       | Required | Default | Description                                                                                               |
|-----------------|----------|---------|-----------------------------------------------------------------------------------------------------------|
| `command`       | Yes      | None    | One of `delete`, `expire`, `flushall`, `get`, `keys`, or `set`                                            |
| `expire`        | No       | None    | The expiration time for records in the Redis database. Defaults to None.                                  |
| `serialization` | No       | False   | When True: data being written will be serialized while data read will be deserialized. Defaults to False. |

#### `command`
The Redis command to be executed. One of `delete`, `expire`, `flushall`, `get`, `keys`, or `set`.

Note that all the directives described in the following sections fall under the `arguments` directive.

For example, the `keys` directive in the `delete` command is `arguments.keys`:
```yaml
redis:
  command: get
  arguments:
    keys:
      - 'my_key'
      - 'my_other_key'
```

##### `delete`
Deletes records from the Redis database.

| Directives | Required | Default | Description                                 |
|------------|----------|---------|---------------------------------------------|
| `keys`     | No       | None    | A list of keys to delete.                   |
| `patterns` | No       | None    | A list of patterns to match keys to delete. |

```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  silo: harvest-jobs
  command: delete
  arguments:
    keys:
      - 'my_key'
      - 'my_other_key'
    patterns:
      - 'some_prefix*'
```

##### `expire`
Sets an expiration time for records in the Redis database in seconds.

| Directives | Required | Default | Description                                                              |
|------------|----------|---------|--------------------------------------------------------------------------|
| `expire`   | No       | None    | The expiration time for records in the Redis database. Defaults to None. |
| `keys`     | No       | None    | A list of keys to expire.                                                |
| `patterns` | No       | None    | A list of patterns to match keys to expire.                              |

```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  silo: harvest-jobs
  command: expire
  arguments:
    expire: 3600
    pattern: 'some_prefix*'
```

##### `flushall`

```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  silo: harvest-jobs
  command: flushall
```

##### `get`
Retrieve records from the Redis database.

| Directives | Required | Default | Description                                   |
|------------|----------|---------|-----------------------------------------------|
| `keys`     | No       | None    | A list of keys to retrieve.                   |
| `patterns` | No       | None    | A list of patterns to match keys to retrieve. |

```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  silo: harvest-jobs
  command: get
  result_as: my_redis_data
  arguments:
    keys:
      - 'my_key'
      - 'my_other_key'
    pattern: 'some_prefix*'
```

##### `keys`
Gets a list of keys from the Redis database.

| Directives | Required | Default | Description                                   |
|------------|----------|---------|-----------------------------------------------|
| `keys`     | No       | None    | A list of keys to retrieve.                   |
| `patterns` | No       | None    | A list of patterns to match keys to retrieve. |

```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  silo: harvest-jobs
  result_as: my_redis_keys
  command: keys
  arguments:
    keys:
      - 'my_key'
      - 'my_other_key'
    pattern: 'some_prefix*'
```

##### `set`
Inserts data into a Redis database.

| Directives | Required    | Default | Description                                                                       |
|------------|-------------|---------|-----------------------------------------------------------------------------------|
| `name`     | Yes         | None    | The record identifier.                                                            |
| `value`    | Yes         | None    | A list of values to set.                                                          |
| `keys`     | Conditional | None    | A list of keys to set. Required when setting multiple values for the same record. |

```yaml
redis:
  name: My Redis Task
  description: This task sets a simple value in a Redis database.
  silo: harvest-jobs
  command: set
  arguments:
      name: 'my_key'
      value: 'my_value'
```

```yaml
redis:
  name: My Redis Task
  description: This task sets the values of my_sub_key and my_other_sub_key in a Redis database.
  silo: harvest-jobs
  command: set
  data: var.my_data
  arguments:
      name: my_record_identifier
      keys:
        - my_sub_key
        - my_other_sub_key
```

```yaml
redis:
  name: My Redis Task
  description: This task uses the iteration directive to set the names and values of multiple records in a Redis database.
  silo: harvest-jobs
  data: var.my_data
  iterate: var.my_data    
  command: set
  arguments:
      name: item.my_data_name
      keys: '*'   # All keys
```


## Example
```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  silo: harvest-jobs
  command: get
  arguments:
    patterns: 'my_key'
```

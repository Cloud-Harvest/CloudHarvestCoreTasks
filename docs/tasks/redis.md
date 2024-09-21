# RedisTask
This task connects to a Redis database and performs some operation.

# Python
## Attributes

> **Note** This Task inherits parameters from the [`BaseDataTask`](base_data.md#the-db-attribute) class, such as `db`.

| Attribute          | Description                                                  |
|--------------------|--------------------------------------------------------------|
| `invalidate_after` | The number of seconds to wait before invalidating the cache. |

## Methods
| Method      | Description                                           |
|-------------|-------------------------------------------------------|
| `method()`  | This is the content of the Task when it is executed.  |

# Configuration
This example demonstrates how to use the RedisTask in a configuration file. Here, `db` is populated with the `alias` key.

```yaml
redis:
  name: My Redis Task
  description: A task which performs a Redis command.
  command: get
  db:
    alias: 'ephemeral'
  key: 'my_key'
  result_attribute: 'value'
```

The `RedisTask` (as well as any Task) can accept user-defined parameters when part of a `report` TaskChain. In this example,
we use the `RedisTask` to retrieve the ongoing status of jobs in the job queue. We then use the `recordset` Task to
apply user-defined filters to the data before outputting the results to the user.

```yaml
report:
  name: Get Job Status
  description: Retrieve the status of jobs in the job queue.
  headers:
    - id
    - status
    - current
    - total
    - percent
    - start
    - end
    - duration
    - counts
  tasks:
    - redis:
        name: Get Job Status
        description: Retrieve the status of jobs in the job queue.
        command: get
        db:
          alias: 'ephemeral'
          database: 'job_queue'
        result_as: 'redis_job_queue'
    - recordset:
        name: Filter Job Status
        description: Filter the job status data.
        data_in: 'redis_job_queue'

```
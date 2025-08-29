# BaseDataTask([BaseTask](./base_task.md))
The BaseDataTask class is a subclass of the [BaseTask](./base_task.md) class.  It represents a task that connects to a data 
provider and performs some action. 

Therefore, the BaseDataTask class should not be used directly. Instead, it should be subclassed to create a new task.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

## Configuration
The `BaseDataTask` class should not be used directly. Instead, it should be subclassed to create a new task.

### Directives

| Directive                   | Required | Default | Description                                                 |
|-----------------------------|----------|---------|-------------------------------------------------------------|
| `command`                   | Yes      | None    | The command to run on the data provider.                    |
| `arguments`                 | No       | None    | Arguments to pass to the command.                           |
| `silo`                      | No       | None    | The name of the silo to use for the task. Defaults to None. |
| `query_retry_max_attempts`  | No       | `3`     | The maximum number of attempts to retry a query.            |
| `query_retry_delay_seconds` | No       | `1.0`   | The number of seconds to wait before retrying a query.      |

## Example
_See [MongoTask](./mongo.md) for an example of a subclass of the BaseDataTask class._

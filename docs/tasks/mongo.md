# MongoTask([BaseDataTask](./base_data.md)) | `mongo`
The MongoTask class is a subclass of the BaseDataTask(BaseTask) class. It represents a task connects to a Mongo
database and performs some action. 

* [Configuration](#configuration-example)
  * [Directives](#directives)
* [Example](#example)

## Configuration

### Directives

| Directive    | Required | Default     | Description                                                                                                                         |
|--------------|----------|-------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `collection` | Yes      | None        | The name of the collection to perform the command on.                                                                               |
| `command`    | No       | `aggregate` | The MongoDb command to execute.                                                                                                     |
| `arguments`  | No       | None        | A dictionary representing arguments to pass to `pymongo`. For instance, when using the `aggregate` command, `pipeline` is required. |

## Example

```yaml
mongo:
  name: My Mongo Task
  description: A task which performs a Mongo command.
  silo: harvest-core
  collection: my_collection
  command: find
  arguments:
    filter:
      "name": "John"     
    projection:
      "name": 1, 
      "age": 1
    sort:
      "age": 1
```

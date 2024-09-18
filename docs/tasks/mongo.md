# MongoTask
The MongoTask class is a subclass of the BaseDataTask(BaseTask) class. It represents a task connects to a Mongo
database and performs some action. 

> This class is primarily used for testing purposes.

# Table of Contents

- [MongoTask](#mongotask)
- [Python](#python)
    - [Attributes](#attributes)
    - [Methods](#methods)
- [Code Example](#code-example)
- [Configuration](#configuration)
    - [Arguments](#arguments)
    - [Example](#example)

# Python
## Attributes

| Attribute         | Description                                                                                                                                                                                                                             | 
|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| command           | The command to run on the Mongo database.                                                                                                                                                                                               |
| db                | A dictionary representing database connection parameters. `host`, `port`, and `database` are required. These values are fed directly into the `MongoClient`, so all applicable keys are valid. `alias: 'persistent'` is permitted here. |
| collection        | The collection to run the command on.                                                                                                                                                                                                   |
| result_attribute  | A command suffix to retrieve, such as `row_count` or `inserted_id`.                                                                                                                                                                     |

## Methods

| Method   | Description                                          | 
|----------|------------------------------------------------------| 
| method() | This is the content of the Task when it is executed. | 

# Code Example
```python 
from CloudHarvestCoreTasks.tasks import MongoTask

task = MongoTask(
    name='My Mongo Task',
    description='A task that does nothing when run.',
    command='find',
    db={
        'host': 'localhost',
        'port': 27017,
        'database': 'my_database',
        'username': 'my_username',
        'password': 'my_password'
    },
    collection='my_collection',
    result_attribute='row_count'
)
```

# Configuration Example

This example demonstrates how to use the MongoTask in a configuration file. Here, `db` is populated the `alias` and `database` keys.
`alias: 'persistent'` is likely the most common use case for MongoTask, as it will be leveraged for reporting. However,
users may wish to connect to other databases for data retrieval or other purposes.
```yaml
mongo:
  name: My Mongo Task
  description: A task which performs a Mongo command.
  command: find
  arguments:
    filter: {"name": "John"}            # All arguments show in this section are for example purposes only.
    projection: {"name": 1, "age": 1}   # Refer to the MongoDB documentation for a full list of available arguments.
    sort: {"name": 1}
    limit: 0
    skip: 0
  db:
      alias: persistent                 # The alias key is used to route the task to the appropriate backend using a connection pool.
      database: my_database
  collection: my_collection
```

This example demonstrates how to use the MongoTask in a configuration file. Here, `db` is populated with specific connection parameters.
```yaml
mongo:
  name: My Mongo Task
  description: A task which performs a Mongo command.
  command: find
  arguments:
    filter: {"name": "John"}            # All arguments show in this section are for example purposes only.
    projection: {"name": 1, "age": 1}   # Refer to the MongoDB documentation for a full list of available arguments.
    sort: {"name": 1}
    limit: 0
    skip: 0
  db:
      host: localhost
      port: 27017
      database: my_database
      username: my_username
      password: my_password
  collection: my_collection
```

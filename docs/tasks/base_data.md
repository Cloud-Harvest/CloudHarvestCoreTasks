# BaseDataTask
The BaseDataTask class is a subclass of the [BaseTask](./base_task.md) class. 
It represents a task that connects to a data provider and performs some action.

- [BaseDataTask](#basedatatask)
- [Python](#python)
    - [Attributes](#attributes)
    - [The `db` Attribute](#the-db-attribute)
    - [Methods](#methods)
- [Code Example](#code-example)
- [Configuration](#configuration)

# Python
## Attributes

| Attribute  | Description                                                | 
|------------|------------------------------------------------------------| 
| command    | The command to run on the Mongo database.                  |
| db         | A dictionary representing database connection parameters.  |
| arguments  | A dictionary of arguments to pass to the command.          |

## The `db` Attribute
The `db` attribute is a dictionary representing database connection parameters. It has two modes of operation:
1. The dictionary contains the connection parameters required to connect to the database. The keys are fed directly into the database connection object, so all applicable keys are valid. Consult the documentation on the associated data provider for more information.
2. The `alias` key is provided with one of two values:
  a. `ephemeral`: The task is routed to the Redis backend used for short-term storage using a connection pool.
  b. `persistent`: The task is routed to the MongoDb backend used for persistent storage using the connection pool.

## Methods

| Method         | Description                                                                                                                       | 
|----------------|-----------------------------------------------------------------------------------------------------------------------------------| 
| connect()      | A method to connect to a the database. Connection Pools should be used wherever possible when constructing BaseDataTask subtasks. |
| disconnect()   | This method disconnects from the database.                                                                                        |
| is_connected() | This method returns a boolean indicating if the task is connected to the database.                                                |
| method()       | A method overwritten by subclasses used to perform some action.                                                                   | 

# Code Example
```python 
from base import BaseDataTask

class CustomDataProvider(BaseDataTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def is_connected(self) -> bool:
        pass
    
    def connect(self):
        pass
    
    def disconnect(self):
        pass
        
    def method(self):
        pass
```

# Configuration
The `BaseDataTask` class should not be used directly. Instead, it should be subclassed to create a new task.

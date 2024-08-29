
# ErrorTask

The `ErrorTask` class is a subclass of the `BaseTask` class. It represents a task that always raises an exception when run. 

> This class is used for testing error handling in task chains.


# Methods

| Method       | Description                               |
|--------------|-------------------------------------------|
| **method()** | Raises an exception to simulate an error. |


# Example

```yaml
error:
    name: My Error Task
    description: A task that always raises an exception when run.
```

```python

from CloudHarvestCoreTasks.tasks import ErrorTask
error_task = ErrorTask()

try:
    error_task.method()

except Exception as e:
    print(e)  # Output: This is an error task
```

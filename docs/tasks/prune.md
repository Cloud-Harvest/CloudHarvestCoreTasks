# PruneTask
The `PruneTask` class is a subclass of the `BaseTask` class. It represents a task that prunes the task chain by clearing the data of previous tasks and/or the stored variables in the task chain.

`PruneTask` is helpful when long-running Task Chains are executed, and the data of previous tasks is no longer needed. By pruning the task chain, the memory usage can be reduced.  

# Attributes
| Attribute              | Description                                                                |
|------------------------|----------------------------------------------------------------------------|
| **previous_task_data** | If True, the data of all previous tasks in the task chain will be cleared. |
| **stored_variables**   | If True, all variables stored in the task chain will be cleared.           |

# Methods
| Method       | Description                                                        |
|--------------|--------------------------------------------------------------------|
| **method()** | Prunes the task chain and returns the instance of the `PruneTask`. |

# Example

## Configuration

```yaml
prune:
    previous_task_data: true
    stored_variables: true
```

## Python

```python
from CloudHarvestCoreTasks.tasks import PruneTask

prune_task = PruneTask(previous_task_data=True, stored_variables=True)
prune_task.method()
print(prune_task.data)  # Output: {'total_bytes_pruned': <number_of_bytes_pruned>}
```
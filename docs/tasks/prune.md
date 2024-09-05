# PruneTask
The `PruneTask` class is a subclass of the `BaseTask` class. It represents a task that prunes the task chain by clearing the data of previous tasks and/or the stored variables in the task chain.

`PruneTask` is helpful when long-running Task Chains are executed, and the data of previous tasks is no longer needed. By pruning the task chain, the memory usage can be reduced.

# Table of Contents

- [PruneTask](#prunetask)
- [Python](#python)
    - [Attributes](#attributes)
    - [Methods](#methods)
    - [Code Example](#code-example)
- [Configuration](#configuration)
    - [Arguments](#arguments)
    - [Example](#example)

# Python

## Attributes
| Attribute              | Description                                                                            |
|------------------------|----------------------------------------------------------------------------------------|
| **previous_task_data** | If True, the data of all previously completed tasks in the task chain will be cleared. |
| **stored_variables**   | If True, all variables stored in the task chain will be cleared.                       |

## Methods
| Method       | Description                                                        |
|--------------|--------------------------------------------------------------------|
| **method()** | Prunes the task chain and returns the instance of the `PruneTask`. |

## Code Example

```python
from CloudHarvestCoreTasks.tasks import PruneTask

prune_task = PruneTask(previous_task_data=True, stored_variables=True)
prune_task.method()
print(prune_task.out_data)  # Output: {'total_bytes_pruned': <number_of_bytes_pruned>}
```

# Configuration

## Arguments
The `PruneTask` class has the following arguments beyond those defined in [BaseTask](./base.md).

| Key                | Required | Default | Description                                                                            |
|--------------------|----------|---------|----------------------------------------------------------------------------------------|
| previous_task_data | No       | False   | If True, the data of all previously completed tasks in the task chain will be cleared. |
| stored_variables   | No       | False   | If True, all variables stored in the task chain will be cleared.                       |

## Example
```yaml
prune:
  previous_task_data: true
  stored_variables: true
```

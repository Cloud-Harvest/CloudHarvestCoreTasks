# DummyTask
The DummyTask class is a subclass of the BaseTask class. It represents a task that does nothing when run.

> This class is primarily used for testing purposes.

# Table of Contents

- [DummyTask](#dummytask)
- [Python](#python)
    - [Attributes](#attributes)
    - [Methods](#methods)
- [Code Example](#code-example)
- [Configuration](#configuration)
    - [Arguments](#arguments)
    - [Example](#example)

# Python
## Attributes

| Attribute | Description                             | 
|-----------|-----------------------------------------| 
| data      | A list containing dummy data.           | 
| meta      | A dictionary containing dummy metadata. |  


## Methods

| Method   | Description                                                                          | 
|----------|--------------------------------------------------------------------------------------| 
| method() | This method does nothing and is used to represent a task that does nothing when run. | 

# Code Example
```python 
from CloudHarvestCoreTasks.tasks import DummyTask

dummy_task = DummyTask()
dummy_task.run()

print(dummy_task.data) # Output: [{'dummy': 'data'}] 
print(dummy_task.meta) # Output: {'info': 'this is dummy metadata'} 
```

# Configuration

## Arguments
The DummyTask class has no arguments beyond those defined in [BaseTask](./base_task).

## Example

```yaml
dummy:
  name: My Dummy Task
  description: A task that does nothing when run.
```
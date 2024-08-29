# DummyTask 
The DummyTask class is a subclass of the BaseTask class. It represents a task that does nothing when run. 

> This class is primarily used for testing purposes.  

# Attributes

| Attribute | Description                             | 
|-----------|-----------------------------------------| 
| data      | A list containing dummy data.           | 
| meta      | A dictionary containing dummy metadata. |  


# Methods 

| Method   | Description                                                                          | 
|----------|--------------------------------------------------------------------------------------| 
| method() | This method does nothing and is used to represent a task that does nothing when run. | 

# Examples

## Configuration
```yaml
dummy:
  name: My Dummy Task
  description: A task that does nothing when run.
```

## Python
```python 
from CloudHarvestCoreTasks.tasks import DummyTask

dummy_task = DummyTask() 
dummy_task.run()

print(dummy_task.data) # Output: [{'dummy': 'data'}] 
print(dummy_task.meta) # Output: {'info': 'this is dummy metadata'} 
```

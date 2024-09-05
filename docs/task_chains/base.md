# BaseTaskChain

The `BaseTaskChain()` class is responsible for managing a chain of tasks. It stores a list of tasks and provides methods 
to run the tasks in the chain, insert new tasks into the chain, and handle completion and error states. It also 
provides properties to track the progress of the task chain.

## Dynamic Configuration
Tasks are templated just before they are run. This allows for dynamic configuration of tasks based on the variables 
provided by previous tasks. Once a Task has been templated, it is instantiated, added to the TaskChain list of tasks,
and run.

## Task Concurrency
Tasks are run sequentially by default. However, setting the `blocking` attribute of any Task to `False` will allow the
TaskChain to run tasks concurrently. The `max_workers` attribute of the TaskChain determines the maximum number of
workers that can be used to run tasks concurrently.

## Python

### Attributes

| Attribute        | Description                                                 |
|------------------|-------------------------------------------------------------|
| `name`           | The name of the task chain.                                 |
| `status`         | The current status of the task chain.                       |
| `variables`      | A dictionary of variables used by the tasks in the chain.   |
| `task_templates` | A list of task configurations to be instantiated and run.   |
| `start`          | The start time of the task chain.                           |
| `end`            | The end time of the task chain.                             |
| `position`       | The current position of the task chain.                     |
| `pool`           | The task pool managing concurrent execution of tasks.       |
| `meta`           | Metadata associated with the task chain.                    |

### Methods

| Method                         | Description                                                                                |
|--------------------------------|--------------------------------------------------------------------------------------------|
| `__init__()`                   | Initializes a new instance of the `BaseTaskChain` class.                                   |
| `__enter__()`                  | Enters the context management protocol.                                                    |
| `__exit__()`                   | Exits the context management protocol.                                                     |
| `data()`                       | Returns the data produced by the last task in the task chain.                              |
| `detailed_progress()`          | Calculates and returns the progress of the task chain.                                     |
| `percent()`                    | Returns the current progress of the task chain as a percentage.                            |
| `performance_metrics()`        | Calculates and returns the performance metrics of the task chain.                          |
| `result()`                     | Returns the result of the task chain.                                                      |
| `total()`                      | Returns the total number of tasks in the task chain.                                       |
| `find_task_by_name()`          | Finds a task in the task chain by its name.                                                |
| `find_task_position_by_name()` | Finds the position of a task in the task chain by its name.                                |
| `get_variables_by_names()`     | Retrieves variables stored in the `BaseTaskChain.variables` property based on their names. |
| `insert_task_after_name()`     | Inserts a new task into the task chain immediately after a task with a given name.         |
| `insert_task_before_name()`    | Inserts a new task into the task chain immediately before a task with a given name.        |
| `insert_task_at_position()`    | Inserts a new task into the task chain at a specific position.                             |
| `on_complete()`                | Method to run when the task chain completes.                                               |
| `on_error()`                   | Method to run when the task chain errors.                                                  |
| `on_start()`                   | Method to run when the task chain starts.                                                  |
| `run()`                        | Runs the task chain. This method will block until all tasks in the chain are completed.    |
| `terminate()`                  | Terminates the task chain.                                                                 |

## Code Examples

```python
from CloudHarvestCoreTasks.base import BaseTaskChain

template = {
    'name': 'My Task Chain',
    'tasks': [
        {
            'task': 'dummy_task',
            'name': 'Dummy Task',
            'description': 'A dummy task that does nothing.'
        },
        {
            'task': 'dummy_task',
            'name': 'Dummy Task 2',
            'description': 'A dummy task that does nothing.'
        }
    ]
}


# Example usage
task_chain = BaseTaskChain(template=template)
task_chain.run()
print()
```

# Configuration
This section details how to use the BaseTaskChain class in a YAML TaskChain configuration.

## Arguments
```yaml
name: My Task Chain
description: A task chain that does nothing.
max_workers: 4
tasks:
  - task: dummy_task
    name: Dummy Task
    description: A dummy task that does nothing.
  - task: dummy_task
    name: Dummy Task 2
    description: A dummy task that does nothing.
```

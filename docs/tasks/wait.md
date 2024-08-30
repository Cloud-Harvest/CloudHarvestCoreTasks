# WaitTask
The `WaitTask` is a task that waits for a specified amount of time or until one of several predefined conditions have been met.

> `wait` with `when_all_previous_async_tasks_complete: True` is implied when a Task is run using `blocking: False` 
> and the TaskChain runs out of new, sequential tasks are run.

# Table of Contents

- [WaitTask](#waittask)
- [Python](#python)
  - [Attributes](#attributes)
  - [Methods](#methods)
  - [Code Example](#code-example)
- [Configuration](#configuration)
  - [Arguments](#arguments)
  - [Example](#example)

# Python

## Attributes

| Attribute                                | Type        | Default | Description                                                                               |
|------------------------------------------|-------------|---------|-------------------------------------------------------------------------------------------|
| `check_time_seconds`                     | float       | 1       | The time interval in seconds at which this task checks if its conditions are met.         |
| `when_after_seconds`                     | float       | 0       | The time in seconds that this task should wait before proceeding.                         |
| `when_all_previous_async_tasks_complete` | bool        | False   | A flag indicating whether this task should wait for all previous async tasks to complete. |
| `when_all_previous_tasks_complete`       | bool        | False   | A flag indicating whether this task should wait for all previous tasks to complete.       |
| `when_all_tasks_by_name_complete`        | List\[str\] | None    | A list of task names. This task will wait until all tasks with these names are complete.  |
| `when_any_tasks_by_name_complete`        | List\[str\] | None    | A list of task names. This task will wait until any task with these names is complete.    |

## Methods

| Method                                   | Description                                                                                          |
|------------------------------------------|------------------------------------------------------------------------------------------------------|
| `__init__`                               | Initializes a new instance of the `WaitTask` class.                                                  |
| `method`                                 | Runs the task. This method will block until the conditions specified by the task attributes are met. |
| `when_after_seconds`                     | Checks if the allotted seconds have passed since this task started.                                  |
| `when_all_previous_async_tasks_complete` | Checks if all previous async tasks are complete.                                                     |
| `when_all_previous_tasks_complete`       | Checks if all previous tasks are complete.                                                           |
| `when_all_tasks_by_name_complete`        | Checks if all tasks with the specified names are complete.                                           |
| `when_any_tasks_by_name_complete`        | Checks if any task with the specified names is complete.                                             |

## Code Example

```python
from CloudHarvestCoreTasks.tasks import WaitTask
task = WaitTask(when_after_seconds=5)
task.run()

print('Waited for 5 seconds.')
```

# Configuration

## Arguments
The following arguments can be used to configure a `WaitTask` beyond those defined in [BaseTask](./base.md).

Note that it is not necessary to supply conditions beyond the one that is immediately required. For example, if the task 
only needs to wait for a specified amount of time, only `when_after_seconds` needs to be supplied.

| Key                                      | Type        | Default | Description                                                                               |
|------------------------------------------|-------------|---------|-------------------------------------------------------------------------------------------|
| `check_time_seconds`                     | float       | 1       | The time interval in seconds at which this task checks if its conditions are met.         |
| `when_after_seconds`                     | float       | 0       | The time in seconds that this task should wait before proceeding.                         |
| `when_all_previous_async_tasks_complete` | bool        | False   | A flag indicating whether this task should wait for all previous async tasks to complete. |
| `when_all_previous_tasks_complete`       | bool        | False   | A flag indicating whether this task should wait for all previous tasks to complete.       |
| `when_all_tasks_by_name_complete`        | List\[str\] | None    | A list of task names. This task will wait until all tasks with these names are complete.  |
| `when_any_tasks_by_name_complete`        | List\[str\] | None    | A list of task names. This task will wait until any task with these names is complete.    |

## Example

```yaml
wait:
  name: My Wait Task
  description: A task that waits for a specified amount of time or until a condition is met.
  check_time_seconds: 1
  when_after_seconds: 5
  when_all_previous_async_tasks_complete: true
  when_all_previous_tasks_complete: false
  when_all_tasks_by_name_complete:
    - task1
    - task2
  when_any_tasks_by_name_complete:
    - task3
    - task4
```

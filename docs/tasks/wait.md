# WaitTask([BaseTask](./base_task.md)) | `wait`
The `WaitTask` is a task that waits for a specified amount of time or until one of several predefined conditions have been met.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

# Configuration

## Directives

| Directive                                | Required | Default | Description                                                                               |
|------------------------------------------|----------|---------|-------------------------------------------------------------------------------------------|
| `check_time_seconds`                     | No       | 1       | The time interval in seconds at which this task checks if its conditions are met.         |
| `when_after_seconds`                     | No       | 0       | The time in seconds that this task should wait before proceeding.                         |
| `when_all_previous_async_tasks_complete` | No       | False   | A flag indicating whether this task should wait for all previous async tasks to complete. |
| `when_all_previous_tasks_complete`       | No       | False   | A flag indicating whether this task should wait for all previous tasks to complete.       |
| `when_all_tasks_by_name_complete`        | No       | None    | A list of task names. This task will wait until all tasks with these names are complete.  |
| `when_any_tasks_by_name_complete`        | No       | None    | A list of task names. This task will wait until any task with these names is complete.    |

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

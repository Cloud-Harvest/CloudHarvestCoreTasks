# Base Task
The BaseTask is the base class for all tasks in the pipeline. It provides the basic functionality for a task, such as 
logging, error handling, and task execution. Many methods in the BaseTask are meant to be overridden by subclasses to
provide custom behavior. The BaseTask is not meant to be used directly, but rather to be subclassed by other tasks.

# Table of Contents

- [Base Task](#base-task)
  - [Python](#python)
    - [Attributes](#attributes)
      - [On](#on)
      - [Retry](#retry)
      - [When](#when)
    - [Methods](#methods)
    - [Code Example](#code-example)
  - [Configuration](#configuration)
    - [Arguments](#arguments)
    - [Example](#example)

# Python
## Attributes

| Attribute           | Description                                                                                                                                                 |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `blocking`          | When `True`, Tasks in the TaskChain wait until this Task completes before continuing. When `False`, the Task will run asynchronously.                       |
| `description`       | A brief description of what the task does.                                                                                                                  |
| `end`               | The end time of the task.                                                                                                                                   |
| `in_data`           | The input data for the task. Commonly used with the `recordset` and `file` Tasks.                                                                           |
| `meta`              | Metadata associated with the task.                                                                                                                          |
| `name`              | The name of the task. It can be anything that helps the user identify what the Task does.                                                                   |
| `on`                | A dictionary of directives to run on specific events (e.g., `error`).                                                                                       |
| `original_template` | The original template of the task configuration. This is used in scenarios where the Task must be templated multiple times, such as with the `ForEachTask`. |
| `out_data`          | The output data of the task.                                                                                                                                |
| `result_as`         | The name under which the result will be stored in the task chain's variables. This makes the output of a Task available to other Tasks in the TaskChain.    |
| `retry`             | A dictionary containing retry configuration.                                                                                                                |
| `start`             | The start time of the task.                                                                                                                                 |
| `status`            | The current status of the task.                                                                                                                             |
| `task_chain`        | The task chain that the task belongs to. Added automatically if the Task is created as part of a chain of Tasks.                                            |
| `when`              | A condition that determines if the task should run.                                                                                                         |
| `with_vars`         | A list of variables from the parent task chain that templated tasks will use.                                                                               |

### On
A dictionary of directives to run on specific events. The key is the event directive and the value is a Task configuration.

| Key        | Description                                                                                                                                                                                                                                                 |
|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `start`    | The Task configuration to run when the task starts.                                                                                                                                                                                                         |
| `complete` | The Task configuration to run when the task completes. Typically, this would just be the next task in the TaskChain however, users can deeply nest Tasks, so it may be advantageous to execute an `on_complete()` after an asynchronous task has completed. |
| `error`    | The Task configuration to run when the task errors.                                                                                                                                                                                                         |
| `skipped`  | The Task configuration to run when the task is skipped.                                                                                                                                                                                                     |

### Retry
A dictionary containing retry configuration.

> Note: Tasks which retry will call `on_start()`, regardless if `on_start()` has already been called. However, 
> `on_complete()`, `on_error()`, and `on_skipped()` will only be called once per task, even if the task is retried, 
> when those method conditions are met.

The retry configuration can contain the following keys:

| Key                   | Default | Description                                                                                                                           |
|-----------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------|
| `delay_seconds`       | 1.0     | The number of seconds to wait before retrying the task.                                                                               |
| `max_attempts`        | 1       | The maximum number of attempts to make before failing the task. A value of 1 means that the task will not be retried.                 |
| `when_error_like`     | None    | When provided, the task will only be retried if the error message is similar to the provided value. This is a regular expression.     |
| `when_error_not_like` | None    | When provided, the task will only be retried if the error message is not similar to the provided value. This is a regular expression. |

### When
A condition that determines if the task should run. The condition is a string that is evaluated as a Python expression using
`jinja2` templating. The task will only run if the condition evaluates to `True`.

## Methods

| Method                | Description                                                                        |
|-----------------------|------------------------------------------------------------------------------------|
| `__init__()`          | Initializes a new instance of the `BaseTask` class.                                |
| `duration`            | Returns the duration of the task.                                                  |
| `position`            | Returns the position of the task in the task chain.                                |
| `method()`            | The main method to be overridden by subclasses to provide specific functionality.  |
| `run()`               | Runs the task.                                                                     |
| `_run_on_directive()` | Runs a directive based on an `on` event.                                           |
| `on_complete()`       | Method to run when a task completes.                                               |
| `on_error()`          | Method to run when a task errors.                                                  |
| `on_skipped()`        | Method to run when a task is skipped.                                              |
| `on_start()`          | Method to run when a task starts but before `method()` is called.                  |
| `terminate()`         | Terminates the task.                                                               |

## Code Example
```python
from CloudHarvestCoreTasks.base import BaseTask

class CustomTask(BaseTask):
    def method(self):
        # Custom task logic here
        self.output = "Task completed"

# Example usage
custom_task = CustomTask(name="Example Task")
custom_task.run()
print(custom_task.output)  # Output: Task completed
```

# Configuration
There is no configuration path for the BaseTask as it should not be called directly. 

## Arguments
The standard arguments for all tasks which inherit BaseTask are as follows:

| Attribute           | Optional | Default     | Description                                                                                                                                                 |
|---------------------|----------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`              | No       | N/A         | The name of the task. It can be anything that helps the user identify what the Task does.                                                                   |
| `blocking`          | Yes      | `False`     | When `True`, Tasks in the TaskChain wait until this Task completes before continuing. When `False`, the Task will run asynchronously.                       |
| `description`       | Yes      | `""`        | A brief description of what the task does.                                                                                                                  |
| `in_data`           | Yes      | `None`      | The input data for the task. Commonly used with the `recordset` and `file` Tasks.                                                                           |
| `on`                | Yes      | `{}`        | A dictionary of directives to run on specific events (e.g., `error`).                                                                                       |
| `result_as`         | Yes      | `""`        | The name under which the result will be stored in the task chain's variables. This makes the output of a Task available to other Tasks in the TaskChain.    |
| `retry`             | Yes      | `{}`        | A dictionary containing retry configuration.                                                                                                                |
| `when`              | Yes      | `""`        | A condition that determines if the task should run.                                                                                                         |
| `with_vars`         | Yes      | `[]`        | A list of variables from the parent task chain that templated tasks will use.                                                                               |

## Example
```yaml
dummy:
  name: My Dummy Task
  description: A task that does nothing when run.
  blocking: True
  retry:
    delay_seconds: 5
    max_attempts: 3
  with_vars:
    - last_task_results
  when: "{{ last_task_results.some_key == 'value' }}"
```
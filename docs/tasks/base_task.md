# Base Task
The BaseTask is the base class for all tasks in the pipeline. It provides the basic functionality for a task, such as
logging, error handling, and task execution. Many methods in the BaseTask are meant to be overridden by subclasses to
provide custom behavior. The BaseTask is not meant to be used directly, but rather to be subclassed by other tasks.

* [Configuration](#configuration)
* [Directives](#directives)
  * [`iterate`](#iterate-directive)
  * [`filters`](#filters-directive)
  * [`on` Directive](#on-directive)
  * [`result_as` Directive](#result_as-directive)
  * [`retry` Directive](#retry-directive)
* [Example](#example)

## Configuration
There is no configuration path for the BaseTask as it should not be called directly.

## Directives
The standard arguments for all tasks which inherit BaseTask are as follows:

| Directive                           | Optional | Default | Description                                                                                                                                                                                 |
|-------------------------------------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`                              | No       | N/A     | The name of the task. It can be anything that helps the user identify what the Task does.                                                                                                   |
| `blocking`                          | Yes      | `False` | When `True`, Tasks in the TaskChain wait until this Task completes before continuing. When `False`, the Task will run asynchronously.                                                       |
| `data`                              | Yes      |         | The input data for the task.                                                                                                                                                                |
| `description`                       | Yes      |         | A brief description of what the task does.                                                                                                                                                  |
| `ignore_filters`                    | Yes      | `False` | Prevents the `filters` directive from being executed.                                                                                                                                       |
| `iterate`                           | Yes      |         | Performs the same task for each item in a variable.                                                                                                                                         |
| [`on`](#on-directive)               | Yes      | `{}`    | A dictionary of directives to run on specific events (e.g., `error`).                                                                                                                       |
| [`result_as`](#result_as-directive) | Yes      |         | The name under which the result will be stored in the task chain's variables. This makes the output of a Task available to other Tasks in the TaskChain.                                    |
| [`retry`](#retry-directive)         | Yes      | `{}`    | A dictionary containing retry configuration.                                                                                                                                                |
| [`filters`](../filters.md)          | Yes      | `{}`    | Modifies the output of some tasks. See [filtering](../filters.md) for more information.                                                                                                     |
| `filterable_fields`                 | Yes      | `[]`    | When provided, only the fields provided in the list will be filtered in this task.                                                                                                          |
| `result_to_dict_key`                | Yes      |         | The results are converted to a dictionary under the specified key.                                                                                                                          |
| `result_to_list_with_key`           | Yes      |         | The list of results is converted to a list of dictionaries under the specified key. When `result_dict_to_keys` is also specified, this directive is performed first is performed **first**. |
| `when`                              | Yes      |         | A condition that determines if the task should run. Uses Jinja2 formatting: `when: {{ var.variable_name == "value" }}`                                                                      |

### `iterate` Directive

| Directive | Optional | Default | Description                                                                                                                                                                                 |
|-----------|----------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`    | No       | N/A     | The name of the task. It can be anything that helps the user identify what the Task does.                                                                                                   |


### `on` Directive
Executes a Task when a specific event occurs. Each event is a key in the dictionary, and the value is a list of Tasks to
run when the event occurs.

| Directive  | Optional | Default | Description                                                                                                                                                                |
|------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `complete` | Yes      |   | A Task to run when the Task completes successfully. This event may be redundant in a Task Chain. Instead, consider simply adding another Task in the chain after this one. |
| `error`    | Yes      |   | A Task to run when the Task encounters an error.                                                                                                                           |
| `skipped`  | Yes      |   | A Task to run when the Task is skipped.                                                                                                                                    |
| `start`    | Yes      |   | A Task to run when the Task starts. This event may be redundant in the Task Chain. Instead, consider simply adding another Task in the chain before this one.              |

```yaml
error:
  name: An Error Task
  description: A task which always fails.
  on:
    error:
     - dummy:
         name: Error Handler Task
         description: A task to run when the Task encounters an error.
```

### `result_as` Directive

#### As a string
The `result_as` directive stores the result of a task in the task chain as a variable which can be referenced later. Using
the `var.` prefix. For example, a `result_as: my_variable` directive can be retrieved in a later task using the
`var.my_variable` value.

> If the same `result_as` name is used in multiple tasks, the value will be overwritten by the most recent task to use that name.

```yaml
tasks:
  - dummy:
      name: My Dummy Task
      description: A task that does nothing when run.
      result_as: my_variable
```

#### As a dictionary

**Directives**

| Directive  | Optional | Default     | Description                                             |
|------------|----------|-------------|---------------------------------------------------------|
| `name`     | No       |             | The variable name to use.                               |
| `mode`     | Yes      | `overwrite` | How assignments of the same variable should be treated. |
| `include`  | Yes      |             | A dictionary of key/values to include in the result.    |

**Modes**

| Directive   | Description                                                                                                                                                               |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `append`    | Appends the result to the existing value of the variable. Expects a list.                                                                                                 |
| `extend`    | Extends the existing value of the variable with the result. Expects a list.                                                                                               |
| `locked`    | Once assigned, the variable cannot be modified by a task which uses the same `result_as` configuration (but may be modified by other tasks which do not specify `locked`) |
| `merge`     | Merges the existing value of the variable with the result. Expects a dictionary.                                                                                          |
| `overwrite` | (default) Overwrites the existing value of the variable with the result. Does not expect any specific type.                                                               |

```yaml
tasks:
  - dummy:
      name: My Dummy Task
      description: A task that does nothing when run.
      result_as:
        name: my_variable
        mode: append
        include:
          - key1: value1
          - key2: value2

```

### `retry` Directive
The `retry` directive allows you to specify how many times a task should be retried and how long to wait between retries.

| Directive             | Optional | Default | Description                                              |
|-----------------------|----------|---------|----------------------------------------------------------|
| `delay_seconds`       | Yes      | `1.0`   | The number of seconds to delay before retrying the task. |
| `max_attempts`        | Yes      | `1`     | The maximum number of times to retry the task.           |
| `when_error_like`     | Yes      | `""`    | A regex pattern to match the error message.              |
| `when_error_not_like` | Yes      | `""`    | A regex pattern to match the error message.              |

```yaml
dummy:
  name: My Dummy Task
  description: A task that does nothing when run.
  retry:
    delay_seconds: 5
    max_attempts: 3
    when_error_like: ".*Some error text here"
```

## Example
The following example describes how to use Tasks in a Report Task Chain to display the status of data collection jobs.
Note that several of the directives mentioned in this document appear in each Task (`redis` and `dataset`). This example
is taken from the [CloudHarvestAgent](https://github.com/Cloud-Harvest/CloudHarvestAgent/tree/main/CloudHarvestAgent/templates/reports/harvest/jobs.yaml).

```yaml
report:
  name: Harvest Jobs
  description: Displays the data collection jobs and their status.
  headers:
    - Start
    - End
    - Id
    - Stage
    - Name
    - Duration
    - Status
    - Errors
    - Records
    - Bytes
    - Description

  tasks:
    # Data retrieval tasks
    - redis:
        name: Get Enqueued tasks
        description: Displays the status of the tasks that are currently awaiting processing.
        result_as: harvest_task_queue
        silo: harvest-task-queue
        command: get
        arguments:
          patterns: "task::*"
        serialization: true

    - redis:
        name: Get Active tasks
        description: Displays the status of the tasks that are currently being processed.
        result_as: harvest_tasks
        silo: harvest-tasks
        command: get
        arguments:
          patterns: "task::*"
        serialization: true

    - redis:
        name: Get Completed tasks
        description: Displays the status of the tasks that have been completed.
        result_as: harvest_task_results
        silo: harvest-task-results
        command: get
        arguments:
          patterns: "*"
          keys:
            - template
            - metrics
            - errors
        serialization: true

    # Data formatting tasks
    - dataset:
        name: Modify enqueued task data
        description: Formats the data from the enqueued tasks.
        data: var.harvest_task_queue
        result_as: harvest_task_queue
        stages:
          - add_keys:
              keys: Stage
              default_value:  enqueued
          - splice_key:
              source_key: _id
              start: 6        # 'task::' is 6 characters long

    - dataset:
        name: Modify processing task data
        description: Formats the data from the processing tasks.
        data: var.harvest_tasks
        result_as: harvest_tasks
        stages:
          - add_keys:
              keys: Stage
              default_value:  processing
          - splice_key:
              source_key: _id
              start: 6
          - nest_keys:
              source_keys: meta

    - dataset:
        name: Modify completed task data
        description: Formats the data from the completed tasks.
        data: var.harvest_task_results
        result_as: harvest_task_results
        stages:
          - add_keys:
              keys: Stage
              default_value:  completed
          - unwind:
              source_key: metrics
          - match_and_remove:
              matching_expressions:
                - 'metrics.Position==Total'
          - rename_keys:
              mapping:
                template.name: Name
                template.description: Description
                metrics.Position: Position
                metrics.Status: Status
                metrics.Records: Records
                metrics.DataBytes: Bytes
                metrics.Duration: Duration
                metrics.Start: Start
                metrics.End: End
          - count_elements:
              source_key: errors
              target_key: Errors

    - dataset:
        name: Consolidate data
        description: Combines the data from the enqueued, active, and completed tasks.
        filters: '.*'
        result_as: harvest_jobs
        stages:
          - add_records:
              records:
                - var.harvest_task_queue
                - var.harvest_tasks
                - var.harvest_task_results
          - rename_keys:
              mapping:
                _id: Id
          - title_keys:
              remove_characters:
                - "_"
```
# CloudHarvestCoreTasks
This repository contains the base and common tasks found in CloudHarvest. Tasks and TaskChains are a fundamental part of the CloudHarvest framework, and are used to define workflows in every layer of the application.

- [CloudHarvestCoreTasks](#cloudharvestcoretasks)
- [Task Chains](#task-chains)
- [Tasks](#tasks)
  - [Tasks Declared in JSON/YAML](#tasks-declared-in-jsonyaml)
  - [Tasks Declared in Python](#tasks-declared-in-python)
- [License](#license)

# Task Chains
A TaskChain is a JSON or YAML file which describes the Tasks to be executed. Task Chains are used to define a workflow. 

Consider this annotated excerpt from the [CloudHarvestApi reports collection](https://github.com/Cloud-Harvest/CloudHarvestApi/blob/main/CloudHarvestApi/api/blueprints/reports/reports/harvest/nodes.yaml):
```yaml
report:                                                         # This is the TaskChain's Chain identifier
  name: 'Report'                                                # Arbitrary name of the TaskChain
  description: |                                                # An arbitrary description of the TaskChain
    This TaskChain generates a 
    report of the API nodes
  tasks:                                                        # This is the list of tasks to be executed
    - cache_aggregate:                                          # This is the first task to be executed
        name: query harvest.api_nodes                           # The first Task's name
        description: 'Get information about the API nodes'      # ...and description                 
        result_as: result                                       # This is the name of the result which will be available to other tasks within the same TaskChain if this Task completes successfully
        on:                                                     # This is a list of tasks to be executed when the task reaches one of four states: complete, error, skipped, and start
          complete:                 
            - task: ...                                         # This is a task to be executed when the task completes
          error:
            - task: ...                                         # This is a task to be executed when the task errors
        when: "{{ var }} == 'value'"                            # A jinja2 template which must evaluate to True in order for the task to run 
```

# Tasks
A Task is the basic unit of work in CloudHarvest. Tasks are designed to be modular and reusable, and are used to build more 
complex workflows (ie `TaskChain`s). Tasks are designed to be as simple as possible, and are generally performed 
sequentially within their TaskChain, although there are specialized Tasks which run asynchronously.

## Tasks Declared in JSON/YAML
A `Task` must always be declared as part of a TaskChain, even if the chain will only contain one Task. Like TaskChains,
Tasks are defined with a name that is lowercase and, where necessary, underscores are used to separate words. All Tasks
should have the following keys available:

| Key           | Example                  | Required | Description                                                                                                                                                                                                                                                                      |
|---------------|--------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`        | "my_task"                | True     | The name of the Task. This should be a short, descriptive name that describes the Task's purpose.                                                                                                                                                                                |
| `description` | "This task..."           | True     | A longer description of the Task, which should provide more detail about what the Task does.                                                                                                                                                                                     |
| `blocking`    | True / False             | False    | When `True` (default), the task prevents subsequent tasks from running until it completes. If `False`, the task will be started immediately as a background thread.                                                                                                              |
| `on`          | `{'directive': [...]}`   | False    | Allows the assignment of additional Tasks when the Task after it reaches one of four states: `complete`, `error`, `skipped`, and `start`. These Tasks are inserted after the current task in the chain and therefore change the total number of Tasks performed for a given run. |
| `result_as`   | "result"                 | False    | The name of the result that the Task will produce. This result will be available to other Tasks in the same TaskChain.                                                                                                                                                           |
| `when`        | "var == 'value'          | False    | A dictionary of variables that the Task requires in order to run. This key is only required if the Task requires variables in order to run.                                                                                                                                      |
| `with_vars`   | `["result1", "result2"]` | False    | A list of variables previously declared as a `result_as` from a previous Task within the same `TaskChain`. This key is only required if the Task requires the results of a previous Task in order to run.                                                                        |


## Tasks Declared in Python
All Tasks inherit from this repository's the `tasks.BaseTask` class, which provides a common interface for all tasks. 
This supplies Tasks with the following methods, all of which can be overridden by subclasses to provide specialized tasks.
That being said, the `method()` method is the primary method to define when authoring `BaseTask` subclasses and most
tasks will function adequately with only this method defined.

| Key             | Description                                                                                            |
|-----------------|--------------------------------------------------------------------------------------------------------|
| `method()`      | A method called by `run()`. This is the primary method to define when authoring `BaseTask` subclasses. |
| `run()`         | This method is called by the TaskChain when the Task is executed.                                      |
| `on_complete()` | A method which is called after the Task has been executed.                                             |
| `on_error()`    | A method which is called if an error occurs during the execution of the Task.                          |
| `on_skipped()`  | A method which is called if the Task is skipped by the `when` option.                                  |
| `on_start()`    | A method which is called before the Task is executed.                                                  |
| `terminate()`   | A method which is called if the Task is terminated before it completes.                                |


# Data Model
Cloud Harvest uses a `List[dict]` data model which is designed to be flexible, extensible, and renderable in many different output formats.

Find more information on the Cloud Harvest Data Model and its methods in the [Data Model README](CloudHarvestCoreTasks/data_model/README.md).

# License
Shield: [![CC BY-NC-SA 4.0][cc-by-nc-sa-shield]][cc-by-nc-sa]

This work is licensed under a
[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License][cc-by-nc-sa].

[![CC BY-NC-SA 4.0][cc-by-nc-sa-image]][cc-by-nc-sa]

[cc-by-nc-sa]: http://creativecommons.org/licenses/by-nc-sa/4.0/
[cc-by-nc-sa-image]: https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png
[cc-by-nc-sa-shield]: https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg
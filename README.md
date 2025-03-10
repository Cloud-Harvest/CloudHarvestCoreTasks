# CloudHarvestCoreTasks
This repository contains the base and common tasks found in CloudHarvest. Tasks and TaskChains are a fundamental part of the CloudHarvest framework, and are used to define workflows in every layer of the application.

- [CloudHarvestCoreTasks](#cloudharvestcoretasks)
- [Terminology](#terminology)
- [Task Chains](#task-chains)
- [Tasks](#tasks)
  - [Available Tasks](#available-tasks)
- [License](#license)

# Terminology
| Term       | Definition                                                                                                                                                                                               |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Harvest    | The name of the project. Harvest is a data collection, processing, and reporting framework.                                                                                                              |
| Task Chain | A YAML file which describes the Tasks to be executed. Task Chains are used to define a workflow.                                                                                                         |
| Task       | The basic unit of work in Harvest. Tasks are designed to be modular and reusable, and are used to build more complex workflows.                                                                          |
| Agent      | A system which performs Task Chains.                                                                                                                                                                     |
| API        | A system which handles requests from users and sends them to Agents.                                                                                                                                     |
| Silo       | A logical grouping of data. Silos are used to separate data from different sources. Harvest natively uses two different kids of Silos: [Mongo](https://www.mongodb.com/) and [Redis](https://redis.io/). |                                         |

# Task Chains
A [TaskChain](docs/task_chains/base.md) is a JSON or YAML file which describes the Tasks to be executed. Task Chains are used to define a workflow.

Consider this annotated excerpt from the [CloudHarvestApi reports collection](https://github.com/Cloud-Harvest/CloudHarvestApi/blob/main/CloudHarvestApi/templates/reports/harvest/nodes.yaml):
```yaml
# The top level key is a TaskChain name
report:
  description: Generates a list of all Agent and API nodes
  headers:                  # Headers are used to describe which keys from the result should be displayed in the report
    - Role                  # and their order
    - Name
    - Ip
    - Port
    - Version
    - Os
    - Python
    - Duration
    - AvailableChains
    - AvailableTasks
    - Start
    - Last

  tasks:
    # The first task is a `redis` task which retrieves information from a Redis instance
    # See the documentation for each task for specific configuration options
    - redis:
        name: Retrieve information on Agent and API nodes
        result_as: harvest_nodes    # Note that the information gathered here is stored in the `harvest_nodes` key
        silo: harvest-nodes
        command: get
        arguments:
          patterns: "*"
        serialization: true

    # The second task is a `dataset` task which formats the data for the report
    - dataset:
        name: Format the data
        data: var.harvest_nodes
        stages:
          # `dataset` tasks are broken into stages which are executed sequentially
          # In the first stage, the `available_chains` key is converted into a \n (default) separated string
          - convert_list_to_string:
                source_key: available_chains
                
          # In the second stage, the `available_tasks` key is converted into a \n (default) separated string
          - convert_list_to_string:
              source_key: available_tasks
            
          # In the final stage, all of the keys are Titled and the `_` character is removed. This formats the keys in a
          # consistent, human-readable way
          - title_keys:
              remove_characters:
                - "_"
```

# Tasks
A Task is the basic unit of work in CloudHarvest. Tasks are designed to be modular and reusable, and are used to build more
complex workflows (ie `TaskChain`s). Tasks are designed to be as simple as possible, and are generally performed
sequentially within their TaskChain, although there are specialized Tasks which run asynchronously.

## Available Tasks
This module provides many common tasks which are used throughout the CloudHarvest application.
These tasks are designed to be modular and reusable, and are used to build more complex workflows.

| Calling Name                            | Class Name             | Description                                                                                                                             |
|-----------------------------------------|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| [BaseTask](docs/tasks/base_task.md)     | `BaseTask`             | All other tasks inherit from the BaseClass. The BaseTask cannot be called directly within a TaskChain's `tasks` list.                   |
| [BaseDataTask](docs/tasks/base_data.md) | `BaseDataTask`         | A Task which uses a `connect()` method to use a remote data source provider, such as a database. `mongo` and `redis` tasks use this.    |
|                                         |                        |                                                                                                                                         |
| [`dataset`](docs/tasks/dataset.md)      | `HarvestRecordsetTask` | A Task which interacts with data stored as a [Harvest Recordset](CloudHarvestCoreTasks/data_model/README.md).                           |
| [`dummy`](docs/tasks/dummy.md)          | `DummyTask`            | A Task which does nothing. This Task is useful for testing and debugging.                                                               |
| [`error`](docs/tasks/error.md)          | `ErrorTask`            | A Task which raises an error. This Task is useful for testing and debugging.                                                            |
| [`file`](docs/tasks/file.md)            | `FileTask`             | A Task which reads or writes a file.                                                                                                    |
| [`http`](docs/tasks/http.md)            | `HttpTask`             | Perform some action involving a url.                                                                                                    |
| [`json`](docs/tasks/json.md)            | `JsonTask`             | Serialize or deserialize a JSON object.                                                                                                 |
| [`mongo`](docs/tasks/mongo.md)          | `MongoTask`            | This Task connects to a Mongo database and performs some action.                                                                        |
| [`prune`](docs/tasks/prune.md)          | `PruneTask`            | A Task which cleans up memory by deleting the results from previous Tasks.                                                              |
| [`redis`](docs/tasks/redis.md)          | `RedisTask`            | This task connects to a Redis database and performs some operation.                                                                     |
| [`wait`](docs/tasks/wait.md)            | `WaitTask`             | A Task which waits for specific conditions to be met before continuing. Useful when running asynchronous Tasks using `blocking: False`. |

# License
Shield: [![CC BY-NC-SA 4.0][cc-by-nc-sa-shield]][cc-by-nc-sa]

This work is licensed under a
[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License][cc-by-nc-sa].

[![CC BY-NC-SA 4.0][cc-by-nc-sa-image]][cc-by-nc-sa]

[cc-by-nc-sa]: http://creativecommons.org/licenses/by-nc-sa/4.0/
[cc-by-nc-sa-image]: https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png
[cc-by-nc-sa-shield]: https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg

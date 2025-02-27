# PruneTask([BaseTask](./base_task.md)) | `prune`
A task that prunes the task chain by clearing the data of previous tasks and/or the stored variables in the task chain.

`PruneTask` is helpful when long-running Task Chains are executed, and the data of previous tasks is no longer needed. 
By pruning the task chain, memory usage can be reduced.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

## Configuration

### Directives
The `PruneTask` class has the following arguments beyond those defined in [BaseTask](./base_task).

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

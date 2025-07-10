# EnqueueTask([BaseTask](./base_task.md)) | `enqueue`
The EnqueueTask is used to add new tasks to the task queue. Use this task when you want need to queue a task from within 
another task in a way that ensures the proper agent is used. An example scenario for this task is the 
`aws.rds.logs-download` report. In this case, a new task is needed for each log file that is to be downloaded, but each
log must be retrieved by an agent configured for the specific platform, account, and region.

## Configuration

### Directives

| Directive   | Required | Default | Description                                                                                                                                                                        |
|-------------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `template`  | Yes      | None    | The name and type of the task template to enqueue. This must be a valid task name available on an agent. If a report, may just provide the report name as `template: report-name`. |
| `api`       | No       | None    | The API configuration to use for the task. If not specified, the calling agent's API configuration will be used. Only necessary if overriding.                                     |
| `variables` | No       | None    | Values to be passed to the new task.                                                                                                                                               |

## Example

```yaml
tasks:
  - enqueue:
      name: Enqueue Task Example
      description: This task enqueues another task.
      template: 
        name: aws.rds._get_logs
        type: report
      api: 
        host: aws-api
        port: 8000
        token: my-api-token
        ssl:
          pem: /path/to/cert.pem
          verify: true
      variables:
        account: 123456789012
        region: us-west-2
        log_file: my-log-file.log
```
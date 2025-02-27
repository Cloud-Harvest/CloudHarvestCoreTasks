# HttpTask([BaseTask](./base_task.md)) | `http`
The HttpTask class is a subclass of the BaseTask class. It represents a task that connects to an HTTP endpoint and 
performs some action.

* [Configuration](#configuration)
  * [Directives](#directives)
* [Example](#example)

## Configuration

### Directives
| Field        | Default            | Required | Description                                                                   |
|--------------|--------------------|----------|-------------------------------------------------------------------------------|
| url          | `None`             | Yes      | The URL of the HTTP(S) endpoint to connect to.                                |
| auth         | `None`             | No       | A dictionary containing appropriate values for the endpoint's authentication. |
| cert         | `None`             | No       | The path to a certificate file to use for SSL verification.                   |
| content_type | `application/json` | No       | The content type of the request.                                              |
| headers      | `None`             | No       | A dictionary of headers to include in the request.                            |
| method       | `get`              | No       | The HTTP method to use.                                                       |
| verify       | `True`             | No       | Whether to verify the SSL certificate of the endpoint.                        |

## Example
```yaml
http:
  name: My Http Task
  description: A task that connects to an HTTP endpoint.
  result_as: my_http_result
  url: https://127.0.0.1:8000/
  auth: 
    username: admin
    password: admin
  cert: /path/to/cert.pem
  content_type: application/json
  method: get
  verify: false
```

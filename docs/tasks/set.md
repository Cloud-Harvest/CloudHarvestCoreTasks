# SetTask([BaseTask](./base_task)) | `set`
This task allows the user to set variables directly within a task. This is useful for setting static values or for 
casting variables that are derived from other variables using templating.

- [Configuration](#configuration)
    - [Directives](#directives)
- [Example](#example)

## Configuration

### Directives

| Key        | Required | Default | Description                                                                                                      |
|------------|----------|---------|------------------------------------------------------------------------------------------------------------------|
| identifier | Yes      | None    | Name of the variable to create. The created variable will be referenced using the `var.<identifier>` convention. |
| value      | Yes      | None    | The value of the variable to set. This can also be a `Jinja2` template, such as `{{datetime_now()}}`.            |
| cast       | No       |         | Change the datatype to another. For example, values provided in `Jinja2` are always strings.                     |
| clobber    | No       | `False` | Whether override the variable if one with the same identifier already exists.                                    |


## Example

```yaml
set:
  identifier: now
  value: "{{ datetime_now() }}"
  cast: datetime
```

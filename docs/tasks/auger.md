# AugerTask
AugerTask is a class that represents a special MongoTask which uses aggregation queries to retrieve information from the
persistent MongoDb database (silo). The AugerTask is distinct from the MongoDb class because it allows inputs from users
to filter, limit, and sort the data.

> **Note**: The AugerTask is only appropriate for use with the persistent MongoDb database. It is not intended for use
> with the ephemeral Redis silo. Instead, use the [RedisTask](./redis.md) for Redis queries and [RecordSetTask](./recordset.md)
> when working with the ephemeral Redis silo.

# Python

## Attributes
Please see the [MongoTask](./mongo.md) documentation for a list of attributes that are shared between the AugerTask and
the MongoTask, such as the required `collection` parameter.

| Attribute             | Description                                                                             | 
|-----------------------|-----------------------------------------------------------------------------------------|
| `pipeline`            | The aggregation pipeline to execute.                                                    |
| `ignore_user_filters` | A boolean value that determines whether or not to ignore user filters.                  |
| `add_keys`            | A list of keys to add to the pipeline which are not already included in the `pipeline`. |
| `count`               | Return the count of records instead of the records themselves.                          |
| `exclude_keys`        | A list of keys to exclude from the pipeline.                                            |
| `headers`             | A list of headers to include in the output. Providing this also sets the Header order.  |
| `limit`               | The maximum number of records to return.                                                |
| `matches`             | A dictionary of key-value pairs to match.                                               |
| `sort`                | A dictionary of key-value pairs to sort by.                                             |
| `title`               | The title of the output.                                                                |

# Configuration

## Arguments
The primary purpose of the `AugerTask` is to allow users to filter, limit, and sort the data returned from the persistent
MongoDb database. The arguments provided here are those necessary for the `AugerTask` to execute the basic pipeline
query. 

Users then interact with the `AugerTask` which accepts their inputs and modifies the pipeline accordingly.

| Argument              | Description                                                            |
|-----------------------|------------------------------------------------------------------------|
| `collection`          | The collection to run the command on.                                  |
| `pipeline`            | The aggregation pipeline to execute.                                   |
| `ignore_user_filters` | A boolean value that determines whether or not to ignore user filters. |
| `limit`               | The maximum number of records to return.                               |
| `title`               | The title of the output.                                               |

## Example

In this example, we see a configuration file that uses the `AugerTask` to query the `pstar` collection in the persistent
MongoDb database.

```yaml
report:
  tasks:
    - auger:
        collection: pstar
        pipeline: 
          "$project": 
            Platform: 1,
            Service: 1,
            Type: 1,
            Account: 1,
            Region: 1,
            Count: 1,
            StartTime: 1,
            EndTime: 1,
            Errors: 1
```

In this example, we show how user filters are applied to the above pipeline. Here, the user supplies the `--matches` flag
to filter the data by the `Platform`, `Service`, `Type`, `Account`, and `Region` fields. Remember that `-m` is a shorthand
for `--matches` and that the `=` operator is a regex expression and `==` is an exact match.

```yaml

```bash
[harvest] report harvest.pstar -m Platform==aws Service==ec2 Type==instance Account=my-account Region==us-east-1
```

```yaml
    - auger:
        collection: pstar
        pipeline: 
          "$project": 
            Platform: 1,
            Service: 1,
            Type: 1,
            Account: 1,
            Region: 1,
            Count: 1,
            StartTime: 1,
            EndTime: 1,
            Errors: 1
          "$match": 
            $and:
              - Platform: "aws"
              - Service: "ec2"
              - Type: "instance"
              - Account: 
                  $regex: "my-account"
              - Region: "us-east-1"
```

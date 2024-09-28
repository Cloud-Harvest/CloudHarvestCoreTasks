# User Filters
Harvest is, by design, a data collecting and reporting framework. To that end, is both unfortunate and unavoidable that
users will wish to interact with the data in unanticipated ways. Therefore, Harvest provides a mechanism for users to
filter outputs in dynamic ways.

Filters are **not** Tasks, but are instead applied to the input or output of a Task, depending on the Task. For example,
a filter applied to a `MongoTask` will be applied to the input of the Task, while a filter applied to a `FileTask` will
be applied to the output of the Task.

# Table of Contents
1. [User-Defined Attributes](#user-defined-attributes)
2. [Templating Attributes](#templating-attributes)

# User-Defined Attributes
These are the filter fields made available to the user. They are defined in the `user_filters` attribute of the Task;
however, it is not necessary to define `user_filters` in the Task template. Instead, `user_filters` is supplied by
the user's client application.

| Attribute      | Example                              | Description                                                                                                                                                                                                                                      |
|----------------|--------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `add_keys`     | `['key1', 'key2']`                   | A list of keys to add to the output. If the key is present in the data, it will be added to the output. Otherwise, the field will be empty. Keys added in this fashion will be placed at the end of the `header` automatically.                  |
| `count`        | `False`                              | If a value is provided here, the output will be a dictionary with a key equal to the input value and a value will be a count of the number of records from the original result dataset.                                                          |
| `exclude_keys` | `['key1', 'key2']`                   | A list of keys to exclude from the output.                                                                                                                                                                                                       |
| `headers`      | `['key1', 'key2']`                   | A list of headers to include in the output. Changing the headers will not change the data, but will change the order and sorting of the data (unless `sort` is provided).                                                                        |
| `limit`        | `10`                                 | If an integer is provided here, the output will be limited to the number of records indicated or the count of records, whichever is smaller.                                                                                                     |
| `matches`      | `[['key1=a'], ['key1=b', 'key2=c']]` | A list of lists of key, operator, and value strings. The first level of lists is an OR operation, and the second level describe an AND operation. In the provided example, the output will be records where `key1=a` OR (`key1=b` AND `key2=c`). |
| `sort`         | `['key1', 'key2:desc']`              | A list of keys to sort the output by. The order of the keys in the list will determine the order of sorting. The default is ascending, but can be changed to descending by adding ':desc' to the key.                                            |

# Templating Attributes
These are the attributes which are used to define the behavior of the User Filters Task. Each Task has its expected 
configuration for when and how to apply the user filters; however, the `attribute` and `on` can be overridden by the 
template author, if desired.

| Attribute             | Default    | Description                                                                                                                                                                                                                                                |
|-----------------------|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `apply_filter_types`  | `None`     | A regex expression which describes which of the above [User-Defined Attributes](#user-defined-attributes) will be applied to the Task. '*' will apply all. This field must be supplied in order to activate user filtering for a Task.                     |

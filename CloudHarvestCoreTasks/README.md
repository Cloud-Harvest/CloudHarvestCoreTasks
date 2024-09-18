| File              | Description                                                                                                       |
|-------------------|-------------------------------------------------------------------------------------------------------------------|
| `__register__.py` | Classes imported here engage the `@register_definition` decorator to register the class with the Plugin Registry. |
| `base.py`         | Basic building blocks of Tasks and Task Chains.                                                                   |
| `chains.py`       | Task Chain classes.                                                                                               |
| `collectors.py`   | Classes that collect data from various sources.                                                                   |
| `exceptions.py`   | Custom exceptions.                                                                                                |
| `factories.py`    | Methods which create Tasks and Task Chains from dictionaries (e.g. from a JSON file).                             |
| `tasks.py`        | Classes that perform operations on data.                                                                          |

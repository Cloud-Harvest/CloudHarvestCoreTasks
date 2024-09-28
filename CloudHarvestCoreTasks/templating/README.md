# Templating
This module provides a templating engine for the CloudHarvest framework. The templating engine is based on the Jinja2 library.

# Table of Contents
1. [filters.py](#filterspy)
2. [functions.py](#functionspy)

# filters.py
This module contains custom filters for the Jinja2 templating engine. The filters are used to format data in the templates.

| Filter                  | Example                              | Description                                                            |
|-------------------------|--------------------------------------|------------------------------------------------------------------------|
| `filter_datetime_now`   | `{{ 'now'\|datetime_now }}`          | Returns the current date and time in the format `YYYY-MM-DD HH:MM:SS`. |
| `filter_datetime_since` | `{{ '2020-01-01'\|datetime_since }}` | Returns the number of days since the date provided.                    |
| `filter_datetime_until` | `{{ '2020-01-01'\|datetime_until }}` | Returns the number of days until the date provided.                    |
| `parse_datetime`        | `{{ '2020-01-01'\|parse_datetime }}` | Returns a datetime object from the string provided.                    |

# functions.py
This module contains the `template_object()` function. This function is used to convert a JSON or YAML representation
of a TaskChain into a TaskChain object.

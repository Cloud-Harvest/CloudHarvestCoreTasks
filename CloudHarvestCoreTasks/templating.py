"""
This module contains functions for rendering templates using the Jinja2 templating engine.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from logging import getLogger
logger = getLogger('harvest')


def template_object(template: Any, variables: dict = None) -> dict:
    """
    Render a template object.

    This function takes a template and a dictionary of variables, and renders the template with these variables.
    If the template is not a string, it is converted to a JSON string before rendering.
    The function uses the Jinja2 templating engine and includes all filters from the `filters` module.

    Args:
        template (Any): The template to render. If not a string, it is converted to a JSON string.
        variables (dict, optional): The variables to use when rendering the template. Defaults to None.

    Returns:
        dict: The rendered template as a dictionary.

    Example:
    >>> template_object(template='{{ variable }}', variables={'variable': 'value'})
    'value'
    """
    result = {}

    from jinja2 import Environment, DictLoader
    from jinja2.exceptions import TemplateError, UndefinedError, TemplateSyntaxError, TemplateRuntimeError

    # If the template is not a string, convert it to a JSON string
    if not isinstance(template, str):
        from json import dumps
        template_to_render = dumps(template, default=str, indent=4)
    else:
        template_to_render = template

    # Create a Jinja2 environment with the template
    environment = Environment(
        loader=DictLoader({'template': template_to_render}),
    )

    # Add all filters from the `filters` module to the environment as filters and globals
    filters = list_filters()
    environment.filters.update(filters)
    environment.globals.update(filters)

    try:
        # Render the template with the provided variables (or an empty dictionary if no variables were provided)
        from json import loads
        rendered = environment.get_template('template').render(**variables or {})
        result = loads(rendered)

    except (TemplateError, UndefinedError, TemplateSyntaxError, TemplateRuntimeError) as e:
        logger.warning(f'Error rendering template: {e}')

    return result

def list_filters() -> dict:
    """
    This function retrieves all the functions in the current module that start with 'filter_'.

    Conventions:
        Functions intending to operate as jinja2 filters must begin with 'filter_'.
        Functions should also be lower case.
        When implementing helpful functions, simply do not include the 'filter_' prefix.

    Returns:
        dict: A dictionary where the keys are the names of the functions (without the 'filter_' prefix)
              and the values are the function objects themselves.
    """

    import inspect
    import sys

    # Get the current module
    module = sys.modules[__name__]

    filter_methods = {
        method[7:]: getattr(module, method)
        for method in dir(module)
        if inspect.isfunction(getattr(module, method))
        and method.startswith('filter_')
    }

    return filter_methods


def parse_datetime(reference_date: (str or datetime) = None, result_tz_aware: bool = True) -> datetime or None:
    """
    This function parses a reference date into a datetime object.

    If the input is a string, it attempts to parse it into a datetime object.
    If the input is already a datetime object, it simply returns the input.
    If no input is provided, it returns the current datetime in UTC.

    If the parsed datetime object is naive (i.e., has no timezone information),
    it sets the timezone to UTC if result_tz_aware is True. If result_tz_aware is False,
    it ensures the datetime object is naive.

    Args:
        reference_date (str or datetime, optional): The date to parse. Defaults to None.
        result_tz_aware (bool, optional): Whether the result should be timezone aware. Defaults to True.

    Returns:
        datetime or None: The parsed datetime object, or None if parsing fails.
    """

    from dateutil.parser import parse

    try:
        # If reference_date is a string, parse it into a datetime object
        if isinstance(reference_date, str):
            result = parse(reference_date)

        # If reference_date is already a datetime object, simply return it
        elif isinstance(reference_date, datetime):
            result = reference_date

        # If no reference_date is provided, return the current datetime in UTC
        else:
            result = datetime.now(tz=timezone.utc)

        # If result_tz_aware is True and the datetime object is naive, set the timezone to UTC
        if result_tz_aware and result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        # If result_tz_aware is False and the datetime object is aware, make it naive
        elif not result_tz_aware and result.tzinfo is not None:
            result = result.replace(tzinfo=None)

        # Returns the result
        return result

    except ValueError as ve:
        # Log an error message if parsing fails
        logger.error(f'Error parsing datetime {str(reference_date)}: {ve.args}')
        return None

# FILTERS
# ======================================================================================================================

def filter_datetime_ago(**timedelta_kwargs) -> datetime:
    """
    This function calculates a datetime in the past from the current datetime.
    Args
        **timedelta_kwargs: Arguments to pass to the timedelta function: days, hours, minutes, seconds, etc.

    Returns
    datetime or str: The calculated datetime.
    """

    now = datetime.now(tz=timezone.utc)
    result = now - timedelta(**timedelta_kwargs)
    
    return result
    
def filter_datetime_since(reference_date: (str or datetime) = None, result_as_string: bool = False, **timedelta_kwargs) -> datetime:
    """
    This function calculates a datetime in the past from a reference date.

    Args:
        reference_date (str or datetime, optional): The reference date. Defaults to None.
        result_as_string (bool, optional): Whether to return the result as a string. Defaults to False.
        **timedelta_kwargs: Arguments to pass to the timedelta function.

    Returns:
        str or datetime: The calculated datetime.
    """

    start_date = parse_datetime(reference_date)

    result = start_date - timedelta(**timedelta_kwargs)

    if result_as_string:
        return result.isoformat()

    else:
        return result


def filter_datetime_until(reference_date: str or datetime = None, **timedelta_kwargs) -> datetime:
    """
    This function calculates a datetime in the future from a reference date.

    Args:
        reference_date (str or datetime, optional): The reference date. Defaults to None.
        **timedelta_kwargs: Arguments to pass to the timedelta function.

    Returns:
        str or datetime: The calculated datetime.
    """

    start_date = parse_datetime(reference_date)

    result = start_date + timedelta(**timedelta_kwargs)

    return result


def filter_datetime_now() -> datetime:
    """
    Returns the current UTC datetime.

    This function returns the current datetime.

    Returns:
        datetime: The current datetime.
    """

    # Get the current datetime
    return datetime.now(tz=timezone.utc)

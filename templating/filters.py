from datetime import datetime
from logging import getLogger

logger = getLogger('harvest')


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
    from datetime import timezone

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


def filter_datetime_since(
        reference_date: (str or datetime) = None,
        result_as_string: bool = False,
        **timedelta_kwargs

) -> (str or datetime):
    """
    This function calculates a datetime in the past from a reference date.

    Args:
        reference_date (str or datetime, optional): The reference date. Defaults to None.
        result_as_string (bool, optional): Whether to return the result as a string. Defaults to False.
        **timedelta_kwargs: Arguments to pass to the timedelta function.

    Returns:
        str or datetime: The calculated datetime.
    """

    from datetime import timedelta

    start_date = parse_datetime(reference_date)

    result = start_date - timedelta(**timedelta_kwargs)

    if result_as_string:
        return result.isoformat()

    else:
        return result


def filter_datetime_until(
        reference_date: (str or datetime) = None,
        result_as_string: bool = False,
        **timedelta_kwargs

) -> (str or datetime):
    """
    This function calculates a datetime in the future from a reference date.

    Args:
        reference_date (str or datetime, optional): The reference date. Defaults to None.
        result_as_string (bool, optional): Whether to return the result as a string. Defaults to False.
        **timedelta_kwargs: Arguments to pass to the timedelta function.

    Returns:
        str or datetime: The calculated datetime.
    """

    from datetime import timedelta

    start_date = parse_datetime(reference_date)

    result = start_date + timedelta(**timedelta_kwargs)

    if result_as_string:
        return result.isoformat()

    else:
        return result


def filter_datetime_now(as_epoc: bool = False, result_tz_aware: bool = True) -> datetime or float:
    """
    Returns the current datetime.

    This function returns the current datetime. If `as_epoc` is set to True, it returns the current datetime as a Unix timestamp.
    If `result_tz_aware` is set to True, the returned datetime object is timezone aware (set to UTC). If False, the datetime object is naive.

    Args:
        as_epoc (bool, optional): If True, returns the current datetime as a Unix timestamp. Defaults to False.
        result_tz_aware (bool, optional): If True, the returned datetime object is timezone aware (set to UTC). If False, the datetime object is naive. Defaults to True.

    Returns:
        datetime or float: The current datetime. If `as_epoc` is True, this will be a Unix timestamp. Otherwise, it will be a datetime object.
    """
    from datetime import datetime, timezone

    # Get the current datetime
    now = datetime.now(tz=timezone.utc) if result_tz_aware else datetime.now()

    # If as_epoc is True, return the current datetime as a Unix timestamp
    if as_epoc:
        return now.timestamp()
    else:
        return now

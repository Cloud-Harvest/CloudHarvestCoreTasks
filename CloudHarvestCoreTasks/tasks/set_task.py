from CloudHarvestCoreTasks.tasks.base import BaseTask
from CloudHarvestCorePluginManager import register_definition

from typing import Any

@register_definition(name='set', category='task')
class SetTask(BaseTask):
    """
    The SetTask class is a subclass of the BaseTask class. It represents a task that sets a variable in the TaskChain's
    variables to a specified value, allowing casting.
    """

    def __init__(self, identifier: str, value: Any, cast: str = None, clobber: bool = False, **kwargs):
        """
        Initializes a new instance of the SetTask class.
        Arguments:
            identifier (str): The name of the variable to set.
            value (Any): The value to set the variable to.
            cast (str, optional): The type to cast the value to. Defaults to None. This means typing is set by the
                PyYAML loader.
            clobber (bool, optional): Whether to overwrite an existing variable with the same name. Defaults to False.
            kwargs: Arbitrary keyword arguments.
        """

        super().__init__(**kwargs)

        self.identifier = identifier
        self.value = value
        self.cast = cast
        self.clobber = clobber

    def method(self, *args, **kwargs) -> 'BaseTask':
        """
        Sets a variable in the TaskChain's variables to the specified value, with optional type casting.
        """

        if self.cast:
            from datetime import datetime, date, time
            types = {
                'str': str,
                'int': int,
                'float': float,
                'bool': bool,
                'date': date,
                'time': time,
                'datetime': datetime
            }

            if self.cast not in types:
                raise ValueError(f'Unsupported cast type: {self.cast}. Must be one of {list(types.keys())}.')

            try:
                match self.cast:
                    # Special handling for boolean casting because bool('False') is True
                    case 'bool':
                        if isinstance(self.value, str):
                            if self.value.lower() in ('true', '1', 'yes'):
                                self.value = True

                            elif self.value.lower() in ('false', '0', 'no'):
                                self.value = False
                            else:
                                # Fallback to default bool conversion
                                self.value = bool(self.value)
                        else:
                            # Use default bool conversion for non-string types
                            self.value = bool(self.value)

                    # Special handling for date/time/datetime casting
                    case ('date' | 'time' | 'datetime'):
                        # First, we need to standardize the input value to a datetime object. This allows us to handle
                        # various input formats (e.g., string, date, time) and then cast to the specific type requested.
                        # We begin by checking if the value is a string, and if so, we attempt to parse it into a
                        # datetime object using dateutil.parser.
                        if isinstance(self.value, str):
                            from dateutil import parser
                            dt_value = parser.parse(self.value)

                        # If already a datetime, preserve it as is.
                        elif isinstance(self.value, datetime):
                            dt_value = self.value

                        # If the input value is a date, we combine it with a default time (midnight) to create a
                        # datetime object.
                        elif isinstance(self.value, date):
                            dt_value = datetime.combine(self.value, time.min)

                        # If the input value is a time, we combine it with a default date (today) to create a
                        # datetime object.
                        elif isinstance(self.value, time):
                            dt_value = datetime.combine(date.today(), self.value)

                        else:
                            raise TypeError(f'Cannot cast value of type {type(self.value).__name__} to {self.cast}.')

                        # Now cast based on the specific type requested.
                        match self.cast:
                            case 'date':
                                self.value = dt_value.date()

                            case 'time':
                                self.value = dt_value.time()

                            case _:
                                self.value = dt_value

                    # Default casting for other types
                    case _:
                        self.value = types[self.cast](self.value)

            except Exception as e:
                raise ValueError(f'Error casting value to {self.cast}: {e}')

        if self.task_chain:
            # Raise error if variable exists and clobber is False
            if self.identifier in self.task_chain.variables and not self.clobber:
                raise ValueError(f'Variable "{self.identifier}" already exists and clobber is set to False.')

            # Assign the value to the TaskChain's variables
            self.task_chain.variables[self.identifier] = self.value

        return self

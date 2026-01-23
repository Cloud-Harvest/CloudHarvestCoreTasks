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
                        from dateutil import parser

                        # Parse the input into a datetime object
                        self.value = parser.parse(self.value)

                        # Convert to the appropriate type, if needed
                        if self.cast == 'date':
                            self.value = self.value.date()

                        elif self.cast == 'time':
                            self.value = self.value.time()

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

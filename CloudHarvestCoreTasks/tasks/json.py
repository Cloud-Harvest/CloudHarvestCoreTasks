from typing import Literal, Any

from CloudHarvestCorePluginManager import register_definition

from CloudHarvestCoreTasks.tasks.base import BaseTask


@register_definition(name='json', category='task')
class JsonTask(BaseTask):
    def __init__(self, mode: Literal['serialize', 'deserialize'], data: Any, default_type: type = str,
                 parse_datetimes: bool = False, *args, **kwargs):
        """
        Initializes a new instance of the JsonTask class.

        Args:
            mode (Literal['serialize', 'deserialize']): The mode in which to operate. 'load' reads a JSON file, 'dump' writes a JSON file.
            data (Any): The data to load or dump. Defaults to None.
            default_type (type, optional): The default type to use when loading JSON data. Defaults to str.
            parse_datetimes (bool, optional): A boolean indicating whether to parse datetimes in the JSON data.
                Attempts to parse a string as a datetime object. If the string cannot be parsed, it is returned as-is.
                Defaults to False.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.data = data
        self.mode = mode
        self.default_type = default_type
        self.parse_datetimes = parse_datetimes

    def method(self) -> 'JsonTask':
        """
        Executes the task.

        Returns:
            JsonTask: The current instance of the JsonTask class.
        """
        def do_mode(data: Any):
            import json
            if self.mode == 'deserialize':
                # Make sure the data is a string, otherwise it has already been deserialized
                if isinstance(data, str):
                    deserialized = json.loads(data)

                else:
                    deserialized = data

                if self.parse_datetimes:
                    def parse_datetime(v: Any):
                        """
                        Attempts to parse a string as a datetime object. If the string cannot be parsed, it is returned as-is.
                        """
                        from datetime import datetime

                        try:
                            return datetime.strptime(v, '%Y-%m-%d %H:%M:%S.%f')

                        except BaseException as ex:
                            return v

                    if isinstance(deserialized, dict):
                        for key, value in deserialized.items():
                            deserialized[key] = parse_datetime(value)

                    elif isinstance(deserialized, list):
                        for i, item in enumerate(deserialized):
                            deserialized[i] = parse_datetime(item)

                    else:
                        deserialized = parse_datetime(deserialized)

                return deserialized


            # Convert the data into a string
            elif self.mode == 'serialize':
                # default=str is used to serialization values such as datetime objects
                # This can lead to inconsistencies in the output, but it is necessary

                return json.dumps(data, default=str)
            return None

        # Check if self.data is an iterable
        if isinstance(self.data, (list, tuple)):
            self.result = [do_mode(d) for d in self.data]

        else:
            self.result = do_mode(self.data)

        return self

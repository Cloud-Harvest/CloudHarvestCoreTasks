from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.dataset import WalkableDict
from CloudHarvestCoreTasks.tasks.base import BaseTask
from CloudHarvestCoreTasks.exceptions import TaskException

from typing import Literal, List


@register_definition(name='file', category='task')
class FileTask(BaseTask):
    """
    The FileTask class is a subclass of the BaseTask class. It represents a task that performs file operations such as
    reading from or writing to a file. Read operations take the content of a file and places them in the TaskChain's
    variables. Write operations take the data from the TaskChain's variables and write them to a file.

    Methods:
        determine_format(): Determines the format of the file based on its extension.
        method(): Performs the file operation specified by the mode and format attributes.
    """

    def __init__(self,
                 path: str,
                 mode: Literal['append', 'read', 'write'],
                 desired_keys: List[str] = None,
                 format: Literal['config', 'csv', 'json', 'raw', 'yaml'] = None,
                 template: str = None,
                 *args, **kwargs):

        """
        Initializes a new instance of the FileTask class.

        Args:
            path (str): The path to the file.
            mode (Literal['append', 'read', 'write']): The mode in which the file will be opened.
            desired_keys (List[str], optional): A list of keys to filter the data by. Defaults to None.
            format (Literal['config', 'csv', 'json', 'raw', 'yaml'], optional): The format of the file. Defaults to None.
            template (str, optional): A template to use for the output. Defaults to None.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """

        super().__init__(*args, **kwargs)
        from pathlib import Path

        # Class-specific
        self.path = path
        self.abs_path = Path(self.path).expanduser().absolute()
        self.mode = mode.lower()
        self.format = str(format or self.determine_format()).lower()
        self.desired_keys = desired_keys
        self.template = template

        # Value Checks
        if self.mode == 'read':
            if self.result_as is None:
                raise TaskException(self, f'The `result_as` attribute is required for read operations.')

    def determine_format(self):
        """
        Determines the format of the file based on its extension.

        Returns:
            str: The format of the file.
        """

        supported_extensions_and_formats = {
            'cfg': 'config',
            'conf': 'config',
            'config': 'config',
            'csv': 'csv',
            'ini': 'config',
            'json': 'json',
            'yaml': 'yaml',
            'yml': 'yaml'
        }

        path_file_extensions = str(self.path.split('.')[-1]).lower()

        return supported_extensions_and_formats.get(path_file_extensions) or 'raw'

    def method(self):
        """
        Performs the file operation specified by the mode and format attributes.

        This method handles both reading from and writing to files in various formats such as config, csv, json, raw, and yaml.

        Raises:
            BaseTaskException: If the data format is not supported for the specified operation.

        Returns:
            self: Returns the instance of the FileTask.
        """
        from configparser import ConfigParser
        from csv import DictReader, DictWriter
        import json
        import yaml

        modes = {
            'append': 'a',
            'read': 'r',
            'write': 'w'
        }

        # Open the file in the specified mode
        with open(self.abs_path, modes[self.mode]) as file:

            # Read operations
            if self.mode == 'read':
                if self.format == 'config':
                    config = ConfigParser()
                    config.read_file(file)
                    result = {section: dict(config[section]) for section in config.sections()}

                elif self.format == 'csv':
                    result = list(DictReader(file))

                elif self.format == 'json':
                    result = json.load(file)

                elif self.format == 'yaml':
                    result = yaml.load(file, Loader=yaml.FullLoader)

                else:
                    result = file.read()

                # If desired_keys is specified, filter the result to just those keys
                if self.desired_keys:
                    def copy_keys_to_dict(record: dict):
                        """
                        Copies the desired keys from the record to a new dictionary.
                        """

                        cktd_result = WalkableDict()

                        for key in self.desired_keys:
                            cktd_result.assign(key, result.get(key))

                        return cktd_result

                    if isinstance(result, dict):
                        self.result = copy_keys_to_dict(result)

                    elif isinstance(result, list):
                        self.result = [
                            copy_keys_to_dict(record)
                            for record in result
                        ]

                # Return the entire result
                else:
                    self.result = result

            # Write operations
            else:

                try:
                    # If the user has specified desired_keys, filter the data to just those keys, if applicable
                    if self.desired_keys:
                        from flatten_json import flatten, unflatten
                        separator = '.'

                        # If the data is a dictionary, flatten it, filter the keys, and unflatten it
                        if isinstance(self.data, dict):
                            self.data = unflatten({
                                k: v for k, v in flatten(self.data.items, separator=separator)()
                                if k in self.desired_keys
                            }, separator=separator)

                        # If the data is a list, flatten each record, filter the keys, and unflatten each record
                        elif isinstance(self.data, list):
                            self.data = [
                                unflatten({
                                    k: v for k, v in flatten(record, separator=separator).items()
                                    if k in self.desired_keys
                                })
                                for record in self.data
                            ]

                    if self.format == 'config':
                        config = ConfigParser()
                        if isinstance(self.data, dict):
                            config.read_dict(self.data)

                            config.write(file)

                        else:
                            raise TaskException(self, f'`FileTask` only supports dictionaries for writes to config files.')

                    elif self.format == 'csv':
                        if isinstance(self.data, list):
                            if all([isinstance(record, dict) for record in self.data]):
                                consolidated_keys = set([key for record in self.data for key in record.keys()])
                                use_keys = self.desired_keys or consolidated_keys

                                writer = DictWriter(file, fieldnames=use_keys)
                                writer.writeheader()

                                writer.writerows(self.data)

                                return self

                        raise TaskException(self, f'`FileTask` only supports lists of dictionaries for writes to CSV files.')

                    elif self.format == 'json':
                        json.dump(self.data, file, default=str, indent=4)

                    elif self.format == 'yaml':
                        yaml.dump(self.data, file)

                    else:
                        file.write(str(self.data))
                finally:
                    from os.path import exists
                    if not exists(self.abs_path):
                        raise TaskException(self, f'The file `{file}` was not written to disk.')

        return self

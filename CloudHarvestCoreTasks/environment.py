"""
The Environment class contains environment variables provided by the user. These variables are loaded by Harvest applications
at runtime from the `/app/env` directory. These variables are accessed in Task configurations using the `env.<var_name>`
syntax, where `<var_name>` is the name of the variable.

The Environment class is a singleton, meaning that there is only one instance of the class in the application.

By design, Environment variables are not intended to be changed once they have been loaded. This is to ensure that the
variables are consistent and do not change unexpectedly during the execution of the application.
"""
from CloudHarvestCoreTasks.dataset import WalkableDict

from logging import getLogger
from typing import Any

logger = getLogger('harvest')


class Environment:
    variables = WalkableDict()

    @staticmethod
    def __getitem__(key: str) -> Any:
        return Environment.variables[key]

    @staticmethod
    def __setitem__(key: str, value: Any) -> None:
        Environment.variables[key] = value

    @staticmethod
    def add(name: str, value: Any, overwrite: bool = False):
        """
        Adds an environment variable to the Environment class.

        Arguments
            name (str): The name of the environment variable.
            value (Any): The value of the environment variable.
            overwrite (bool, optional): Whether to overwrite the variable if it already exists. Defaults to False.

        Returns
            None
        """

        if name not in Environment.variables or overwrite:
            Environment.variables[name] = value

    @staticmethod
    def get(name: str, default: Any = None) -> Any:
        """
        Retrieves the value of an environment variable from the Environment class.

        Arguments
            name (str): The name of the environment variable.
            default (Any, optional): The default value to return if the variable does not exist. Defaults to None.

        Returns
            Any: The value of the environment variable, or the default value if it does not exist.
        """

        return Environment.variables.get(name) or default

    @staticmethod
    def load(path: str) -> None:
        """
        Loads environment variables from a file into the Environment class. Accepted file formats are '.yaml' and '.json'.

        Arguments
            path (str): The path to the file containing the environment variables.

        Returns
            None
        """

        try:
            from os.path import exists

            if not exists(path):
                raise FileNotFoundError(f"File {path} does not exist.")

            with open(path, 'r') as file:
                if path.endswith('.yaml') or path.endswith('.yml'):
                    from yaml import load, FullLoader
                    Environment.variables |= load(file, Loader=FullLoader)

                elif path.endswith('.json'):
                    from json import load
                    Environment.variables |= load(file)

                else:
                    raise ValueError("Unsupported file format. Only '.yaml', '.yml', and '.json' are supported.")

        except Exception as e:
            logger.warning(f'Failed to load environment variables from {path}: {e}')

        else:
            logger.info(f'Environment: successfully loaded {path}')

    @staticmethod
    def purge():
        """
        Clears all environment variables from the Environment class. This method is intended for testing and not
        recommended for production use.

        Returns
            None
        """

        Environment.variables.clear()

    @staticmethod
    def remove(name: str) -> Any:
        """
        Removes an environment variable from the Environment class.

        Arguments
            name (str): The name of the environment variable to remove.

        Returns
            Any: The value of the removed variable, or None if the variable did not exist.
        """

        if name in Environment.variables:
            return Environment.variables.pop(name)

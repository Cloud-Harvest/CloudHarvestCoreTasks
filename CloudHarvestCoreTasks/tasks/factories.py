"""
factories.py - This module contains functions for creating task chains from files or dictionaries.
"""
from logging import getLogger
from typing import Any
from .base import BaseTaskChain, BaseTask

logger = getLogger('harvest')


def task_chain_from_file(file_path: str) -> BaseTaskChain:
    """
    Create a TaskChain from a json or yaml file. The preferred and recommended file type is yaml. The decision
    to prefer YAML over JSON is based on the fact that YAML is (typically) more Human-readable than JSON. Additionally,
    the ability to use anchors and references in YAML can make it easier to create complex data structures.

    The preceding being true, it is acknowledged that a JSON structure most closely resembles MongoDb syntax. As
    CloudHarvest uses a MongoDb backend, JSON may be more familiar to some users or even preferred when authoring
    task chains which will leverage MongoDb-orientated Tasks and TaskChains.

    Args:
        file_path: json or yaml file to load

    Returns:
        BaseTaskChain
    """

    from os.path import expanduser

    # Load the task chain from the file.
    if file_path.endswith('.json'):
        from json import load

        with open(expanduser(file_path), 'r') as file:
            task_chain = load(file)

    elif file_path.endswith('.yaml') or file_path.endswith('.yml'):
        from yaml import load, FullLoader

        with open(expanduser(file_path), 'r') as file:
            task_chain = load(file, Loader=FullLoader)

    else:
        raise ValueError('Unsupported file type. Supported types are .json, .yaml, and .yml.')

    task_chain = task_chain_from_dict(task_chain_registered_class_name=file_path, template=task_chain)

    return task_chain


def task_chain_from_dict(template: dict, **kwargs) -> BaseTaskChain:
    """
    Creates a task chain from a dictionary.

    This function takes a dictionary representation of a task chain and the name of the task chain class to create, and
    returns an instance of that class.

    Arguments
    template (dict): A dictionary representation of the task chain.
    **kwargs: Additional keyword arguments to pass to the task

    Returns:
    BaseTaskChain: An instance of the specified task chain class, initialized with the information from the provided
    dictionary.
    """

    from CloudHarvestCorePluginManager.registry import Registry

    # Identify the class and configuration for the task chain.
    try:
        # The chain class is the first key in the dictionary which does not begin with '.'. We allow templates to include
        # keys that begin with '.' to allow for YAML anchors and references, in addition to metadata keys.
        task_chain_registered_class_name = [key for key in template.keys() if not key.startswith('.')][0]
        task_chain_configuration = template[task_chain_registered_class_name]

    except IndexError:
        from .base import BaseTaskException
        raise BaseTaskException('No task chain class found in the task chain configuration.')

    # Attempt to locate the identified class in the registry.
    try:
        chain_class = Registry.find(result_key='cls',
                                    category='chain',
                                    name=task_chain_registered_class_name)[0]

    except IndexError:
        from .base import BaseTaskException
        raise BaseTaskException(f'No task chain class found for {task_chain_registered_class_name}.')

    # Set the name of the task chain if it is not already set.
    if 'name' not in task_chain_configuration.keys():
        task_chain_configuration['name'] = task_chain_registered_class_name

    # Instantiate the task chain class.
    result = chain_class(template=task_chain_configuration, **task_chain_configuration | kwargs)

    return result


def task_from_dict(task_configuration: dict or BaseTask,
                   task_chain: 'BaseTaskChain' = None,
                   item: Any = None) -> BaseTask:
    """
    Instantiates a task based on the task configuration.

    This method converts a task configuration into an instantiated class. If a task chain is
    provided, it retrieves the variables from the task chain and uses them to template the task configuration.
    The templated configuration is then used to instantiate the task.

    Returns:
        BaseTask: The instantiated task.
    """

    # If the task configuration is already an instantiated task, return it.
    if isinstance(task_configuration, BaseTask):
        return task_configuration

    # If the task configuration is a dictionary, extract the class name and template the configuration.
    class_name = list(task_configuration.keys())[0]

    from CloudHarvestCorePluginManager.registry import Registry
    task_class = Registry.find(result_key='cls', category='task', name=class_name)[0]

    # Replace string object references with the objects themselves
    templated_task_configuration = walk_and_replace(obj=task_configuration, task_chain=task_chain, item=item)

    # Instantiate the task with the templated configuration and return it
    class_configuration = templated_task_configuration.get(class_name) or {}
    instantiated_class = task_class(task_chain=task_chain, **class_configuration)

    instantiated_class.original_template = task_configuration[class_name]

    return instantiated_class
#
#
# def replace_vars_in_dict(nested_dict: dict, vars: dict):
#     """
#     Walks through a nested dictionary and replaces strings starting with 'var.' with the corresponding value
#     from the vars dictionary.
#
#     Args:
#         nested_dict (dict): The nested dictionary to process.
#         vars (dict): The task chain containing the variables.
#
#     Returns:
#         dict: The processed dictionary with replaced values.
#     """
#
#     def replace_vars(value):
#         """
#         Recursively replaces 'var.' strings with the corresponding value from task_chain.variables.
#         """
#
#         if isinstance(value, str) and value.startswith('var.'):
#             return vars.get(value[4:], value)
#
#         elif isinstance(value, dict):
#             return {k: replace_vars(v) for k, v in value.items()}
#
#         elif isinstance(value, list):
#             return [replace_vars(item) for item in value]
#
#         else:
#             return value
#
#     return replace_vars(nested_dict)


def replace_variable_path_with_value(original_string: str,
                                     task_chain: BaseTaskChain = None,
                                     item: (dict or list or tuple) = None,
                                     fail_on_unassigned: bool = False,
                                     **kwargs) -> Any:
    """
    Accepts a path like 'item.key[index].key' and returns the value of that path in the task chain's variables.

    Args:
        original_string (str): The path to the variable in the task chain's variables.
        task_chain (BaseTaskChain): The task chain containing 'var' variables.
        item (dict, list, tuple): When iteration is in effect, this is the item being iterated over.
        fail_on_unassigned (bool): If True, the method will raise an exception if the variable is not assigned.
        **kwargs (dict): Additional keyword arguments to pass to the replace_variable_path_with_value() method.
    """

    from re import compile, split

    """
    Regex expression breakdown:
        (item|var): Matches the literal strings "item" or "var".
        \.: Matches a literal dot.
        [^\s]*: Matches zero or more characters that are not whitespace.
    """

    # If the original string is not a string or does not start with 'var' or 'item', return the original string
    if not isinstance(original_string, str) or not any([f'{prefix}.' in original_string for prefix in ('item', 'var')]):
        return original_string

    pattern = compile('(item|var)\.[^\s]*')

    # Find all the matches in the path
    matches = [match.group(0) for match in pattern.finditer(original_string)]

    # Determines if the entire string will be replaced by the output. When True, the output will be
    # a single value. Otherwise, the output will be a string with the replaced values. This allows users to
    # concatenate strings with variables.
    replace_string_with_value = (len(matches) == 1 and original_string == matches[0])

    def walk_path(p, obj) -> Any:
        """
        Walks the path of the object to retrieve the value at the end of the path.
        """
        # Convert the path string into a list of keys and indices
        path = []

        # Splits the string at either a dot (.) or any substring enclosed in square brackets ([]),
        # while keeping the delimiters (dot or square brackets) in the result.
        parts = split(r'(\[.*?\]|\.)', p)

        # The start_index is assigned based on the type of variable (item or var). This is necessary because the
        # variable identifier (item/var) is not a valid part of the object itself.
        if parts[0] == 'item':
            start_index = 2

        elif parts[0] == 'var':
            start_index = 3

        else:
            start_index = 0

        for part in parts[start_index:]:
            if part == '' or part == '.':
                continue

            if part.startswith('[') and part.endswith(']'):
                path.append(int(part[1:-1]))
            else:
                path.append(part)

        # Traverse the object using the parsed path
        for p in path:

            # Special functions which can be added at the end of the path
            if isinstance(p, str):  # and p.endswith('()'):
                # TODO: I think it would be valuable if we could include the args/kwargs in the function call in ().

                match p:
                    case 'value':  # ()':
                        pass    # We'll return this obj

                    case _:
                        # Remove the () from the end of the string
                        # p = p[:-2]

                        # Check if this is a property or method
                        if hasattr(obj, p):
                            obj = getattr(obj, p)

                            # If the object is a callable, execute it otherwise return the object
                            if callable(obj):
                                obj = obj()

                                # Convert the object to a list if it is a dict_keys or dict_values object
                                if type(obj) in (type({}.keys()), type({}.values())):
                                    obj = list(obj)

                        # If the object is a dict, check that p is a key in the dict
                        elif isinstance(obj, dict):
                            obj = obj[p]

                        # If the object is a list, check that p is an int and within the bounds of the list
                        elif isinstance(obj, list) and isinstance(p, int):
                            obj = obj[p]


            else:
                obj = obj[p]

        return obj

    # Prepare the replacement values dictionary
    replacement_values = {}

    for match in matches:
        # The type of variable (item, var) and the name of the variable
        ms = match.split('.')

        var_type = ms[0]
        var_name = ms[1]

        match var_type:
            # Use the iterator as the source object
            case 'item':
                replacement_values[match] = walk_path(match, item)

            case 'task':
                if task_chain:
                    replacement_values[match] = walk_path(match, task_chain)

            # Get a component of a task chain variable
            case 'var':
                var = task_chain.variables.get(var_name) if task_chain is not None else None

                if var:
                    # Assign the value to the replacement_values dictionary
                    replacement_values[match] = walk_path(match, var)

                else:
                    # From an internal code perspective there may be times when we need to raise errors when a variable
                    # is not assigned. However, in the context of templating a task chain, we may want to ignore this
                    # error and continue. This is especially useful when we are scanning the task chain configurations
                    # for variables that need to be replaced. We may not have assigned the variable yet.
                    if fail_on_unassigned:
                        # Raise an error because a var was referenced but there is no associated task chain
                        if not task_chain:
                            raise ReferenceError(f'Variable "{var_name}" referenced but process is not part of a task chain.')

                        # Raise an error because the var was not found in task_chain.variables
                        if not var:
                            raise ValueError(f'Variable "{var_name}" is not assigned in the task chain. '
                                             f'Did you remember to assign in a previous task with "result_as: var.{var}"?')

                    else:
                        # Normally we will ignore this variable and continue because the variable may not have been
                        # assigned yet. Configurations may be scanned by this method several times before the variable
                        # is assigned.
                        continue

            case _:
                # We shouldn't actually see this error, but it is here just in case.
                raise ValueError(f'Invalid path: {match}; must begin with "item" or "var".')

    # Perform the replacement
    if replace_string_with_value:
        if replacement_values.values():
            # Since the only value of the original_string is a variable reference, we will return the actual
            # variable's object.
            result = list(replacement_values.values())[0]

        # No replacement value was retrieved, so we will return the original string
        else:
            result = original_string

    else:
        # Copy the original string so that we don't mangle the input
        from copy import copy
        result = copy(original_string)

        # Replace the variables in the string
        for key, value in replacement_values.items():
            result = result.replace(key, str(value))

    return result

def walk_and_replace(obj: Any, **kwargs) -> Any:
    """
    Recursively walks through a nested list of dictionaries and lists, executing replace_variable_path_with_value()
    whenever a string is encountered.

    Args:
        obj (Any): The object to process.
        **kwargs: Keyword arguments to pass to replace_variable_path_with_value().

    Returns:
        The processed object with replaced strings.
    """

    if isinstance(obj, dict):
        return {
            k: walk_and_replace(obj=v, **kwargs)
            for k, v in obj.items()
        }

    elif isinstance(obj, list):
        return [
            walk_and_replace(obj=elem, **kwargs)
            for elem in obj
        ]

    elif isinstance(obj, str):
        return replace_variable_path_with_value(original_string=obj, **kwargs)

    else:
        return obj

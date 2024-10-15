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

    task_chain = task_chain_from_dict(task_chain_registered_class_name=file_path, task_chain=task_chain)

    return task_chain


def task_chain_from_dict(task_chain_registered_class_name: str,
                         task_chain: dict,
                         extra_vars: dict = None,
                         **kwargs) -> BaseTaskChain:
    """
    Creates a task chain from a dictionary.

    This function takes a dictionary representation of a task chain and the name of the task chain class to create, and
    returns an instance of that class.

    Parameters:
    task_chain_registered_class_name (str): The name of the task chain.
    task_chain (dict): The dictionary representation of the task chain. This should include all the necessary
                       information to create the task chain, such as the tasks to be executed and their order.
    extra_vars (dict): A dictionary of extra variables to be passed to the task chain.

    Returns:
    BaseTaskChain: An instance of the specified task chain class, initialized with the information from the provided
    dictionary.
    """

    from CloudHarvestCorePluginManager.registry import Registry

    try:
        chain_class = Registry.find(result_key='cls',
                                    category='chain',
                                    name=task_chain_registered_class_name)[0]

    except IndexError:
        from .base import BaseTaskException
        raise BaseTaskException(f'No task chain class found for {task_chain_registered_class_name}.')

    # Set the name of the task chain if it is not already set.
    if 'name' not in task_chain.keys():
        task_chain['name'] = task_chain_registered_class_name

    # Instantiate the task chain class.
    result = chain_class(template=task_chain, extra_vars=extra_vars, **kwargs)

    return result


def task_from_dict(task_configuration: dict or BaseTask,
                   task_chain: 'BaseTaskChain' = None,
                   template_vars: Any = None) -> BaseTask:
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
    class_name = list(task_configuration.redis_keys())[0]

    from CloudHarvestCorePluginManager.registry import Registry
    task_class = Registry.find(result_key='cls', category='task', name=class_name)[0]

    if isinstance(template_vars, dict):
        template_vars = template_vars

    elif isinstance(template_vars, (list, tuple)):
        template_vars = [{'i': value} for value in template_vars]

    else:
        logger.warning(f'Unsupported template_vars type: {type(template_vars)}. Must be a dict, list, or tuple.')

        template_vars = {}

    from .templating import template_object

    # Template the task configuration with the variables from the task chain
    templated_task_configuration = template_object(template=task_configuration,
                                                   variables=template_vars,
                                                   task_chain_vars=task_chain.variables)

    # If this template is part of a TaskChain and there are task variables, add those object to the template
    # based on the object pointer format `var.variable_name`. This is only necessary when these conditions are met
    # because no variables can be added to a task when it is not part of the chain *and* there is no point in
    # flattening the template if there are no variables to add.
    if task_chain is not None and task_chain.variables:
        templated_task_configuration = replace_vars_in_dict(templated_task_configuration, task_chain.variables)

    # Instantiate the task with the templated configuration and return it
    class_configuration = templated_task_configuration.get(class_name) or {}
    instantiated_class = task_class(task_chain=task_chain, **class_configuration)

    instantiated_class.original_template = task_configuration[class_name]

    return instantiated_class


def replace_vars_in_dict(nested_dict: dict, vars: dict):
    """
    Walks through a nested dictionary and replaces strings starting with 'var.' with the corresponding value
    from the vars dictionary.

    Args:
        nested_dict (dict): The nested dictionary to process.
        vars (dict): The task chain containing the variables.

    Returns:
        dict: The processed dictionary with replaced values.
    """

    def replace_vars(value):
        """
        Recursively replaces 'var.' strings with the corresponding value from task_chain.variables.
        """

        if isinstance(value, str) and value.startswith('var.'):
            return vars.get(value[4:], value)

        elif isinstance(value, dict):
            return {k: replace_vars(v) for k, v in value.items()}

        elif isinstance(value, list):
            return [replace_vars(item) for item in value]

        else:
            return value

    return replace_vars(nested_dict)

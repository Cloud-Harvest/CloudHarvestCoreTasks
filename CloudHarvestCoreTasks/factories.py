"""
factories.py - This module contains functions for creating task chains from files or dictionaries.
"""
from logging import getLogger
from typing import Any

from CloudHarvestCoreTasks.tasks.base import BaseTask
from CloudHarvestCoreTasks.chains.base import BaseTaskChain

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
        from CloudHarvestCoreTasks.exceptions import BaseHarvestException
        raise BaseHarvestException('No task chain class found in the task chain configuration.')

    # Attempt to locate the identified class in the registry.
    try:
        chain_class = Registry.find(result_key='cls',
                                    category='chain',
                                    name=task_chain_registered_class_name)[0]

    except IndexError:
        from CloudHarvestCoreTasks.exceptions import BaseHarvestException
        raise BaseHarvestException(f'No task chain class found for {task_chain_registered_class_name}.')

    # Set the name of the task chain if it is not already set.
    if 'name' not in task_chain_configuration.keys():
        task_chain_configuration['name'] = task_chain_registered_class_name

    # Instantiate the task chain class.
    result = chain_class(template=task_chain_configuration, **task_chain_configuration | kwargs)

    return result


def template_task_configuration(task_configuration: dict or BaseTask,
                                task_chain: 'BaseTaskChain' = None,
                                item: Any = None,
                                instantiate: bool = True) -> BaseTask or dict:
    """
    Instantiates a task based on the task configuration.

    This method converts a task configuration into an instantiated class.

    Arguments
        task_configuration (dict or BaseTask): The task configuration.
        task_chain (BaseTaskChain, optional): The task chain associated with the task. Defaults to None.
        item (Any, optional): The current item in the iteration. Defaults to None.
        instantiate (bool, optional): Whether to instantiate the task. Defaults to True. When False, a dictionary is
        returned. The default of 'True' is used because this is the most common scenario for this method.

    Returns
        BaseTask: The instantiated task.
    """

    # If the task configuration is already an instantiated task, return it.
    if isinstance(task_configuration, BaseTask):
        return task_configuration

    # If the task configuration is a dictionary, extract the class name and template the configuration.
    class_name = list(task_configuration.keys())[0]

    from CloudHarvestCorePluginManager.registry import Registry
    if class_name == 'part':
        # The 'part' task is not a real task. Instead, it is a placeholder for a template of another task stored in
        # a templates/parts directory. Parts reduce toil by allowing users to create reusable templates for tasks, such
        # as the steps necessary to retrieve AWS tags using the separate list_tags_for_resource call, which involves
        # multiple steps.
        task_class = Registry.find(result_key='cls', category='template_parts', name=task_configuration[class_name]['part_name'])[0]

    else:
        # Normal task lookup
        task_class = Registry.find(result_key='cls', category='task', name=class_name)[0]

    # Replace string object references with the objects themselves
    from CloudHarvestCoreTasks.dataset import WalkableDict
    from CloudHarvestCoreTasks.environment import Environment
    templated_task_configuration = WalkableDict(task_configuration).replace(
        variables={
            'chain': task_chain,                                                # The task chain itself
            'env': Environment.get(),                                           # All Environment variables
            'item': {'value': item} if isinstance(item, str) else item,         # The current item in the iteration
            'var': task_chain.variables if task_chain is not None else {},      # The task chain variables
        }
    )

    if instantiate:
        # Instantiate the task with the templated configuration and return it
        class_configuration = templated_task_configuration.get(class_name) or {}

        if isinstance(task_chain, BaseTaskChain):
            class_configuration = task_chain.filters | class_configuration

        instantiated_class = task_class(task_chain=task_chain, **class_configuration)

        instantiated_class.original_template = task_configuration[class_name]

        return instantiated_class

    else:
        return templated_task_configuration

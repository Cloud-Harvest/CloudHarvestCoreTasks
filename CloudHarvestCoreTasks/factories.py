from .base import BaseTaskChain


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

    task_chain = task_chain_from_dict(task_chain_name=file_path, task_chain=task_chain)

    return task_chain


def task_chain_from_dict(task_chain_name: str,
                         task_chain: dict,
                         extra_vars: dict = None,
                         **kwargs) -> BaseTaskChain:
    """
    Creates a task chain from a dictionary.

    This function takes a dictionary representation of a task chain and the name of the task chain class to create, and
    returns an instance of that class.

    Parameters:
    task_chain_name (str): The name of the task chain.
    task_chain (dict): The dictionary representation of the task chain. This should include all the necessary
                       information to create the task chain, such as the tasks to be executed and their order.
    extra_vars (dict): A dictionary of extra variables to be passed to the task chain.

    Returns:
    BaseTaskChain: An instance of the specified task chain class, initialized with the information from the provided
    dictionary.
    """

    from CloudHarvestCorePluginManager.registry import Registry

    # Lookup the class for the provided task chain name by scanning the PluginRegistry.
    formal_chain_class_name = task_chain_name.title().replace('_', '') + 'TaskChain'
    try:
        chain_class = Registry.find_definition(class_name=formal_chain_class_name,
                                               is_subclass_of=BaseTaskChain)[0]

    except IndexError:
        from .exceptions import BaseTaskException
        raise BaseTaskException(f'No task chain class found for {task_chain_name} / {formal_chain_class_name}.')

    # Set the name of the task chain if it is not already set.
    if 'name' not in task_chain.keys():
        task_chain['name'] = task_chain_name

    # Instantiate the task chain class.
    result = chain_class(template=task_chain, extra_vars=extra_vars, **kwargs)

    return result

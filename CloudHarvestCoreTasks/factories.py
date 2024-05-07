from .base import BaseTaskChain


def task_chain_from_file(file_path: str, chain_class_name: str):
    """
    Create a task chain from a yaml file.
    Args:
        file_path: The yaml file to load.
        chain_class_name: The name of the task chain class to create.

    Returns:
        BaseTaskChain
    """
    from os.path import expanduser
    from yaml import load, FullLoader

    with open(expanduser(file_path), 'r') as file:
        task_chain = load(file, Loader=FullLoader)

    task_chain = task_chain_from_dict(task_chain_name=file_path, task_chain=task_chain, chain_class_name=chain_class_name)

    return task_chain


def task_chain_from_dict(task_chain_name: str,
                         task_chain: dict,
                         chain_class_name: str,
                         extra_vars: dict = None,
                         **kwargs) -> BaseTaskChain:
    """
    Creates a task chain from a dictionary.

    This function takes a dictionary representation of a task chain and the name of the task chain class to create, and returns an instance of that class.

    Parameters:
    task_chain_name (str): The name of the task chain.
    task_chain (dict): The dictionary representation of the task chain. This should include all the necessary information to create the task chain, such as the tasks to be executed and their order.
    chain_class_name (str): The name of the task chain class to create. This class should be a subclass of the BaseTaskChain class.

    Returns:
    BaseTaskChain: An instance of the specified task chain class, initialized with the information from the provided dictionary.

    Example:
    >>> task_chain_from_dict('myTaskChain', {'tasks': [{'task1': {...}}, {'task2': {...}}]}, 'ReportTaskChain')
    <ReportTaskChain object at 0x7f8b2c3b3d60>
    """

    from .base import TaskRegistry
    chain_class = TaskRegistry.get_task_class_by_name(target_name=chain_class_name,
                                                      target_task_type='taskchain')

    if 'name' not in task_chain.keys():
        task_chain['name'] = task_chain_name

    return chain_class(template=task_chain, extra_vars=extra_vars, **kwargs)

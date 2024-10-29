"""
This module contains the Silo class. A silo is a storage location for data which can be retrieved using CloudHarvest.
"""

class Silo:
    """
    This class is used to define a Silo. A Silo is a storage location for data which can be retrieved using CloudHarvest.
    Silos are accessible via the CloudHarvestPluginManager and the report syntax where the `silo` directive parameter
    is available.

    This class does not provide any connection or storage functionality. It is a definition only used to complete the
    configuration of a reporting task which must access the silo in question.
    """

    def __init__(self,
                 name: str,
                 host: str,
                 port: int,
                 engine: str,
                 username: str = None,
                 password: str = None,
                 database:str = None,
                 **additional_database_arguments):

        self.name = name
        self.host = host
        self.port = port
        self.engine = engine
        self.username = username
        self.password = password
        self.database = database
        self.additional_database_arguments = additional_database_arguments or {}

    def __dict__(self):
        return {
            'name': self.name,
            'host': self.host,
            'port': self.port,
            'engine': self.engine,
            'username': self.username,
            'password': self.password,
            'database': self.database
        } | self.additional_database_arguments

_SILOS = {}

def add_silo(**kwargs):
    """
    Add a silo to the silo registry.

    :param silo: The silo to add.
    """
    silo = Silo(**kwargs)

    if silo.name is not None:
        _SILOS[silo.name] = silo

def clear_silos():
    """
    Clear all silos from the silo registry.
    """
    _SILOS.clear()

def drop_silo(name: str):
    """
    Remove a silo from the silo registry.

    :param name: The name of the silo to remove.
    """
    if name in _SILOS:
        _SILOS.pop(name)

def get_silo(name: str) -> Silo:
    """
    Retrieve a silo by name.

    :param name: The name of the silo to retrieve.
    :return: The silo object.
    """
    return _SILOS.get(name)


"""
The __register__.py file is used to import all the classes from the tasks, collectors, and chains modules. By importing
all the classes here, we engage the Python interpreter to load all the classes when the module is imported. Doing so
kicks off the @register_definition decorator, which registers all the classes with the TaskRegistry. This is necessary
to ensure that the TaskRegistry is populated with all the classes that are available to the system.
"""

from CloudHarvestCoreTasks.silos import (
    MongoSilo,
    RedisSilo
)

from CloudHarvestCoreTasks.base import BaseTaskChain
from CloudHarvestCoreTasks.chains import ReportTaskChain
from CloudHarvestCoreTasks.tasks import (
    DataSetTask,
    DummyTask,
    ErrorTask,
    FileTask,
    HarvestUpdateTask,
    HttpTask,
    JsonTask,
    MongoTask,
    PruneTask,
    RedisTask,
    WaitTask
)

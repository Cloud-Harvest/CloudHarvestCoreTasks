"""
This module contains the core tasks that are used by the CloudHarvest system. The import statements in this module are
used to import all the task classes from the tasks module. This is done to ensure that all the task classes are
registered with the TaskRegistry when the module is imported. This is necessary to ensure that the TaskRegistry is
populated with all the classes that are available to the system.
"""

from .base import (
    BaseTask,
    BaseTaskChain,
    TaskStatusCodes
)

from .chains import (
    ReportTaskChain
)

from .factories import *

from .tasks import (
    DummyTask,
    ErrorTask,
    FileTask,
    ForEachTask,
    HarvestRecordSetTask,
    MongoTask,
    PruneTask,
    RedisTask,
    WaitTask
)

"""
This module initializes the data model for the CloudHarvestCoreTasks package.

It imports the necessary components for data matching and record handling.

Modules:
    matching: Contains classes and functions for matching operations.
    recordset: Contains the HarvestRecordSet class for handling sets of records.
    record: Contains the HarvestRecord class for handling individual records.

Classes:
    HarvestRecordSet: A class for handling sets of records.
    HarvestRecord: A class for handling individual records.
"""

from .matching import *
from .recordset import HarvestRecordSets, HarvestRecordSet, HarvestRecord

# Changelog

## 0.6.5
- [#23](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/23) - Error Message Improvements
  - Improves error messages in `BaseTaskChain` and `BaseTask`
  - Improves error messages in other tasks by calling `TaskError` instead of `Exception`

## 0.6.4
- Part of the [Redis Task Standardization Effort](https://github.com/Cloud-Harvest/CloudHarvestAgent/issues/8)
- Updated `BaseTaskChain` 
  - Added `parent` and `chain_type` arguments
  - Added `redis_` properties
  - Added `update_status()` which is now responsible for sending status reports to Redis (instead of the Agent)
- The task chain factory now populates `chain_type` from the chain's original template
- Refactored `RedisTask` to be more flexible
  - Added Redis helper methods for (un)formatting `HSET` and `HGET` style commands
  - Rewrote documentation for this task
- `HarvestUpdateTask` will now make sure the `Harvest.UniqueIdentifier` key is uniquely indexed on the target and `metadata` collections
- Corrected a syntax error in `MongoTask`'s sort method

## 0.6.3
- Added the Environment package which provides a way to store configuration information made available to tasks
- Added the `CachedData` class providing the means to store data temporarily
- Various improvements to the task instantiation and templating process
- `BaseTask`
  - Added the `include` directive to `result_as`
  - Fixed several missing catches for `result_as
- `BaseTaskChain` 
  - Fixed a bug in  where a null result would the `results_to_silo()` to fail
  - Fixed an issue in `performance_report()` where `min` and `max` on date fields could raise a `ValueError`
  - Fixed an `IndexError` in the `results` property which prevented the chain from returning the result`
- `DataSet` methods
  - Added `split_key_to_keys()`
  - Added `count_elements()`
  - Added `join()`
  - Added `create_index()`, `drop_index()`, `find_index()`, and `refresh_index()` methods
  - Fixed `title_keys()` by removing the `requires_flatten()` decorator and cleaned up the code
- `WalkableDict`
  - Added `replace()` which replaces string values with respective values in a dictionary
  - Fixed an issue in `walk()` where indices would not transverse lists/tuples 
- Fixed an issue in `MongoTask` where the `!=` filter did not properly exclude data
- Fixed issues in `WalkableDict.walk()` 
  - which retreated `0` values as `None`
  - which did not properly walk integer values
- Various improvements to the `BaseHarvestTaskChain` and `HarvestUpdateTask` classes


## 0.6.2
- Updated to CloudHarvestCorePluginManager 0.5.0

## 0.6.1
- Added the `map()` method `DataSet` and `WalkableDict` 

## 0.6.0
- Huge refactor of the directory structure using absolute imports
- Fixed some recursive import issues
- Added `BaseFilterableTask` which some `BaseTasks` will use to implement filtering on a per-Task basis
- `ReportTaskChain` now calculates headers which are returned as metadata to the user
- Simplified Filtering by moving the logic to the `BaseFilterableTask`

## 0.5.1
- Expanded `DataSet` with new maths methods
  - `maths_keys()` performs a mathematical operation on multiple keys in a record and assigns the result to a new key.
  - `maths_records()` performs a mathematical operation on all values for one or more keys and places the output in the maths_results attribute for later retrieval
  - `append_record_maths_results()` appends the maths_results to the record
- Added the `HttpTask`
- `BaseTaskChain.results` now returns more keys: `data`, `errors`, `meta`, `metrics`, and `template`
- `ReportTaskChain` now adds `headers` to the `meta` attribute
- Vastly improved how filtering is applied
  - Created new classes for different types of filters: `DataSetFilter`, `MongoFilter`, and `SqlFilter`
  - Filtering methods are now defined and ordered using the `BaseFilter.ORDER_OF_OPERATIONS` tuple
  - Filters call `apply()` which iterates based on the `ORDER_OF_OPERATIONS`
- Added a performance optimization for `WalkableDict` where walking logic is now bypassed if now separator is in the key name
- `DataSet.sort()` now correctly sorts by multiple keys and in reverse order
- Refactor of the project structure, removing nested directories which were overcomplicating imports

## 0.5.0
- Replaced `HarvestRecordSet` with `DataSet(List[WalkableDict])`
  - Most methods now return a `DataSet` object
  - All operational methods are now stored under this object (whereas `HarvestRecordSet` and `HarvestRecord` split the methods based on how the object was manipulated)
  - These changes simplified `DataSetTask` which replaces `HarvestRecordSetTask`
- `WalkableDict` is a dictionary that can be accessed using dot notation
  - We use different method names to avoid collision with existing `dict` methods
  - `assign()` provides an interface to change the value of a nested key
  - `drop()` pops a nested key from the dictionary
  - `walk()` returns the value of a nested key
- `HarvestRecordSetTask` has been replaced by `DataSetTask`
- The `recordset` task chain directive has been replaced by `dataset`

## 0.4.3
- Added `drop_silos(*names)` to the Silos
- Added `HarvestAgentBlueprint` and `HarvestApiBlueprint` objects
- Updated MongoTask silo usage code
- Updated to Python 3.13
- Moved test data from seed files to `tests/data.py` for easier implementation
- Tests should now leverage default CloudHarvestStack configurations instead of internally managed `tests/docker-compose.yaml`
- `task_chain_from_dict()` now merges `**kwargs` with the task chain configuration on class instantiation
- Removed the `HeartBeat` class. Moved `silos.py` into the root package directory
- Added methods to `HarvestRecord`
  - `assign_value_to_key()`
  - `title_keys()`
- TaskStatusCodes
  - added `get_codes()`
  - Removed Enum; now uses string constants
- HarvestRecordSet
  - Added `nest()` and `unnest()` methods
  - Added `values_to_list()` and `values_from_list_to_str()` methods
  - Removed `remove_key()` in favor of `remove_keys()` which accepts a list of keys
- Bugs
  - Fixed an issue where vars were not properly replaced when they were non-None items

## 0.4.2
- Added `BaseHarvestTaskChain` and `HarvestRecordUpdateTask` to upload data collected from other sources to a persistent silo
- `BaseTaskChain` now accepts starting `variables` which become available to all tasks in the chain
- `additional_database_arguments` have been renamed to `extended_db_configuration` for Silos and DataTasks
- `task_chain_from_dict()`
  - now expects the task chain to begin with a key representing the task chain's class such as `chain`, `harvest`, or `report`
  - all tests updated accordingly

## 0.4.1
- [#12](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/12) Completely redesigned how objects are templated variables referenced in task configurations
- [#13](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/13) Added the `BaseTask.itemize` directive 
  - Allows for the iteration of a task over a list of items 
  - In addition to `var.variable_name`, itemization provides the `item.item_name` for referencing the current item in the iteration.
- Added [`silos`](./CloudHarvestCoreTasks/silos) to record remote data source information in the application
- Tasks will now record multiple errors in sequence using key `self.meta['Errors']` as a list
- Removed the `()` requirement for variable method references in task configurations such as `var.result.keys()` to `var.result.keys`
- Added the `Heartbeat` object for monitoring the health of the application
- Added `add_indexes` to `BaseSilo` to add indexes to a database, where supported
- Updated to CloudHarvestCorePluginManager 0.1.5

## 0.4.0
- Moved MongoDb operations into Tasks since both the Api and Agent will need to interact with the database
- Moved `BaseCacheTask` and `CacheAggregateTask` to `CloudHarvestCoreTasks` as refactors inheriting `BaseDataTask`
- Created the `BaseDataTask` which interacts with database backends
- Created the `MongoTask` and `RedisTask` which inherit from `BaseDataTask`
- TaskChains will now report progress to the ephemeral cache when the `TaskChain.cache_progress` parameter is set to `True`
- Caching (ephemeral and persistent) is now handled in [`silos`](CloudHarvestCoreTasks/silos)
  - The [ephemeral silo](CloudHarvestCoreTasks/silos/ephemeral.py) is a Redis backend
  - The [persistent silo](CloudHarvestCoreTasks/silos/persistent.py) is a MongoDB backend
- Data model Matching logic improvements
  - SQL is now supported
  - Added helper functions `build_mongo_matching_syntax()` and `build_sql_matching_syntax()`
  - Expanded tests
- Added some more documentation
- Improved the efficiency of `HarvestMatchSet` and `HarvestMatch` by instantiating them once, then applying the `match()` method to the data
- Removed the `in_data` and `out_data`
  - Results are now stored in the `result` attribute
  - Previously stored TaskChain data is now accessible using the `var.variable_name` format which is applied at the time of instantiation
- Created [user_filters](CloudHarvestCoreTasks/user_filters) which are used to filter data based on user input
- Updates to support [CloudHarvestCorePluginManager 0.3.0](https://github.com/Cloud-Harvest/CloudHarvestCorePluginManager/tree/v/0.3.0)
- Fixed an issue where `BaseTask._run_on_directive()` was templating objects when it should simply return the configuration back to the TaskChain for templating.

## 0.3.5
- [#9](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/9) c
- Updated `WaitTask` to accept `when_after_seconds`
- Removed `DelayTask` because it is now redundant with the `when_after_seconds` directive in `WaitTask`
- Updated tests
- Expanded documentation which is now stored in `docs/`
- Replaced `BaseTask.data` with `BaseTask.in_data` and `BaseTask.out_data`-
- Added a check in `BaseTaskChain` which cleanly exits the loop when there are no more tasks to instantiate

## 0.3.3
- Implemented `ForEachTask` which replaces `TemplateTask` and tests

## 0.3.2
- Removed `BaseAsyncTask` and associated tests because it is now redundant
- Added `blocking` parameter to `BaseTask` so now any arbitrary task can be run asynchronously
- Create the `BaseTaskPool` class to manage asynchronous tasks
- Fixed some inconsistent tests with `FileTask`

## 0.3.1
- [#3](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/3) Added `when` conditional operator to `BaseTask` and `BaseTaskChain`
- [#5](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/5) Added `FileTask`
- [#6](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/6) Merged CloudHarvestCoreDataModel into this repository
- [#7](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/7) Added `on_` state directives to the `BaseTask`

## 0.2.7
- Updated to conform with CloudHarvestCorePluginManager 0.2.4

## 0.2.6
- `BaseTaskChain.performance_report`
  - Removed several unnecessary metrics
  - Improved metrics and reduced the number of list comprehensions
  - Added a buffer line between the task list and the total row

## 0.2.5
- Added `.__str__()` to TaskStatusCode calls in the `BaseTaskChain.performance_report` output because the class is not JSON serializable

## 0.2.4
- Added missing `end` timestamp in `BaseTaskChain.on_complete()`

## 0.2.3
- Changed the return type of `BaseTaskChain.performance_report` to `List[dict]`
- Added the hostname to the `BaseTaskChain.performance_report` output
- Updated tests

## 0.2.2
- New tests
- PruneTask now returns the `total_bytes_pruned` value
- Removed some unused imports

## 0.2.1
- Updated to conform with CloudHarvestCorePluginManager 0.2.0
  - Implemented `@register_definition` and `@register_instance` decorators
  - Replaced `PluginRegistry` with `Registry`
  - Replaced `find_classes()` with `find_definition()` (classes) and `find_instance()` (instantiated classes)
  - Added `__register__.py`
  - Updated testing
- Resolved issues with `BaseTask.duration`
- Added this CHANGELOG

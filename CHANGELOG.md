# 0.4.1
- Completely redesigned how objects are templated variables referenced in task configurations
- Added the `BaseTask.itemize` directive 
  - Allows for the iteration of a task over a list of items 
  - In addition to `var.variable_name`, itemization provides the `item.item_name` for referencing the current item in the iteration.
- Added [`silos`](./CloudHarvestCoreTasks/silos) to record remote data source information in the application
- Tasks will now record multiple errors in sequence using key `self.meta['Errors']`

# 0.4.0
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

# 0.3.5
- [#9](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/9) c
- Updated `WaitTask` to accept `when_after_seconds`
- Removed `DelayTask` because it is now redundant with the `when_after_seconds` directive in `WaitTask`
- Updated tests
- Expanded documentation which is now stored in `docs/`
- Replaced `BaseTask.data` with `BaseTask.in_data` and `BaseTask.out_data`-
- Added a check in `BaseTaskChain` which cleanly exits the loop when there are no more tasks to instantiate

# 0.3.3
- Implemented `ForEachTask` which replaces `TemplateTask` and tests

# 0.3.2
- Removed `BaseAsyncTask` and associated tests because it is now redundant
- Added `blocking` parameter to `BaseTask` so now any arbitrary task can be run asynchronously
- Create the `BaseTaskPool` class to manage asynchronous tasks
- Fixed some inconsistent tests with `FileTask`

# 0.3.1
- [#3](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/3) Added `when` conditional operator to `BaseTask` and `BaseTaskChain`
- [#5](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/5) Added `FileTask`
- [#6](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/6) Merged CloudHarvestCoreDataModel into this repository
- [#7](https://github.com/Cloud-Harvest/CloudHarvestCoreTasks/issues/7) Added `on_` state directives to the `BaseTask`

# 0.2.7
- Updated to conform with CloudHarvestCorePluginManager 0.2.4

# 0.2.6
- `BaseTaskChain.performance_report`
  - Removed several unnecessary metrics
  - Improved metrics and reduced the number of list comprehensions
  - Added a buffer line between the task list and the total row

# 0.2.5
- Added `.__str__()` to TaskStatusCode calls in the `BaseTaskChain.performance_report` output because the class is not JSON serializable

# 0.2.4
- Added missing `end` timestamp in `BaseTaskChain.on_complete()`

# 0.2.3
- Changed the return type of `BaseTaskChain.performance_report` to `List[dict]`
- Added the hostname to the `BaseTaskChain.performance_report` output
- Updated tests

# 0.2.2
- New tests
- PruneTask now returns the `total_bytes_pruned` value
- Removed some unused imports

# 0.2.1
- Updated to conform with CloudHarvestCorePluginManager 0.2.0
  - Implemented `@register_definition` and `@register_instance` decorators
  - Replaced `PluginRegistry` with `Registry`
  - Replaced `find_classes()` with `find_definition()` (classes) and `find_instance()` (instantiated classes)
  - Added `__register__.py`
  - Updated testing
- Resolved issues with `BaseTask.duration`
- Added this CHANGELOG

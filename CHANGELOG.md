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

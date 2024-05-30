# 2024-05-29
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

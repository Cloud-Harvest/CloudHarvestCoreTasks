# HarvestUpdateTask([BaseTask](./base_task.md)) | `harvest_update`
Updates the Harvest persistence silo (typically `harvest-core`) with the PSTAR data. This task is automatically called
at the conclusion of a [`harvest` Task Chain](../task_chains/base_harvest).

This task has no directives beyond the standard `BaseTask` directives and **must** be called from a `BaseHarvestTaskChain` class such as [`harvest`](../task_chains/base_harvest).

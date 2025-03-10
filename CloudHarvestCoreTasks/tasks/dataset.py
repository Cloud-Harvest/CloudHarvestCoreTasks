from CloudHarvestCorePluginManager import register_definition
from CloudHarvestCoreTasks.tasks.base import BaseFilterableTask
from CloudHarvestCoreTasks.exceptions import TaskException

from typing import List, Any

@register_definition(name='dataset', category='task')
class DataSetTask(BaseFilterableTask):
    """
    DataSetTasks provide a way to manipulate data in a task chain. They are be used to modify, filter, sort, and
    limit data.
    """

    def __init__(self, stages: List[dict], data: Any = None, *args, **kwargs):
        """
        Constructs a new HarvestRecordSetTask instance.

        Arguments
        stages (List[dict]): A list of dictionaries containing the function name and arguments to be applied to the recordset.
        >>> stages = [
        >>>     {
        >>>         'function_name': {
        >>>             'argument1': 'value1',
        >>>             'argument2': 'value2'
        >>>         }
        >>>     }
        >>> ]
        data (Any, optional): The record set to operate on. Defaults to None.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        from CloudHarvestCoreTasks.dataset import DataSet
        self.data = data if isinstance(data, DataSet) else DataSet().add_records(data)
        self.stages = stages
        self.stage_position = 0

    def method(self):
        """
        Executes functions on the recordset with the provided function and arguments, then stores the result in the data attribute.

        This method iterates over the `stages` defined for this task. For each stage, it retrieves the function and its arguments.
        It then checks if the function is a method of the HarvestRecordSet or HarvestRecord class. If it is, it applies the function to the record set or each record in the record set, respectively.
        If the function is not a method of either class, it raises an AttributeError.

        The result of applying the function is stored in the data attribute of the HarvestRecordSetTask instance.

        Returns:
            self: Returns the instance of the HarvestRecordSetTask.
        """

        from CloudHarvestCoreTasks.dataset import DataSet

        for stage in self.stages:
            try:
                # Each dictionary should only contain one key-value pair
                for function, arguments in stage.items():

                    # This is a HarvestRecordSet command
                    if hasattr(DataSet, function):
                        # We don't template RecordSet commands because they are not intended to be used with record-level data
                        getattr(self.data, function)(**(arguments or {}))

                    else:
                        raise TaskException(self, f"Command '{function}' does not exist for the DataSetTask.")

            except Exception as ex:
                raise TaskException(self, f"Error executing stage [{self.stages.index(stage) + 1}] {list(stage.keys())[0]}: {str(ex)}")

            # Increment the stage_position
            self.stage_position += 1

        # Apply user filters
        self.apply_filters()

        # Sets the headers to the filtered keys
        self.headers = self.filter_keys()

        # Assigns the modified DataSet to the result
        self.result = self.data

        return self

    def apply_filters(self) -> 'DataSetTask':
        """
        Applies user filters to the Task. The default user filter class is HarvestRecordSetUserFilter which is executed
        when on_complete() is called. This method should be overwritten in subclasses to provide specific functionality.
        """

        # If the user filters not configured for this Task, return
        if self.filters is None or self.ignore_filters:
            pass

        else:
            super().apply_filters()

        return self

    def _filter_add_keys(self, *args, **kwargs) -> 'DataSetTask':
        """
        This method returns the keys to be added to the data. If the keys already exist, their existing values are preserved.
        """
        if self.add_keys:
            self.data.add_keys(keys=self.add_keys, clobber=False)

        return self

    def _filter_count(self, *args, **kwargs) -> int or None:
        """
        This method returns the count of the data.
        """

        if self.count:
            self.data = len(self.data)

    def _filter_exclude_keys(self, *args, **kwargs) -> 'DataSetTask':
        """
        This method returns the keys to be excluded from the data.
        """

        if self.exclude_keys:
            self.data.drop_keys(keys=self.exclude_keys)

        return self

    def _filter_headers(self, *args, **kwargs) -> 'DataSetTask':
        """
        This method returns the headers of the data.
        """
        self.data.set_keys(keys=list(self.filter_keys()))

        return self

    def _filter_limit(self, *args, **kwargs) -> 'DataSetTask':
        """
        This method returns the limit of the data.
        """
        if self.limit:
            self.data.limit(self.limit)

        return self

    def _filter_matches(self, *args, **kwargs) -> 'DataSetTask':
        """
        This method returns the matches of the data.
        """

        if self.matches:
            self.data.match_and_remove(matching_expressions=self.matches)

        return self

    def _filter_sort(self) -> 'DataSetTask':
        """
        This method returns the sort of the data based on the provided sort or keys(). When keys() is used, each key is
        sorted in the default sort order (ascending).
        """

        self.data.sort_records(keys=self.sort or self.filter_keys())

        return self

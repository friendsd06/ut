from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional, Any, Union
import logging


class DependencyError(Exception):
    pass


class CycleError(DependencyError):
    pass


class InvalidTaskError(DependencyError):
    pass


class TaskProcessor:

    def __init__(self, logger: Optional[logging.Logger] = None):

        self.task_info: Dict[str, Dict[str, Any]] = {}
        self.logger = logger or logging.getLogger(__name__)

    def validate_input_data(self, pipeline_data: List[Dict[str, Any]], task_metadata: List[Dict[str, Any]]) -> None:

        if not isinstance(pipeline_data, list) or not isinstance(task_metadata, list):
            raise InvalidTaskError("Pipeline data and task metadata must be lists.")

        if not pipeline_data:
            raise InvalidTaskError("Pipeline data cannot be empty.")

        required_pipeline_fields = {'pipelineid', 'taskid', 'dependenttaskid'}
        required_metadata_fields = {'taskid', 'operator', 'dependencytype'}

        # Validate pipeline data
        for idx, task in enumerate(pipeline_data, start=1):
            if not isinstance(task, dict):
                raise InvalidTaskError(f"Invalid task format at index {idx}: {task}")
            missing_fields = required_pipeline_fields - task.keys()
            if missing_fields:
                raise InvalidTaskError(f"Missing required fields in task at index {idx}: {missing_fields}")

        # Validate task metadata
        for idx, metadata in enumerate(task_metadata, start=1):
            if not isinstance(metadata, dict):
                raise InvalidTaskError(f"Invalid metadata format at index {idx}: {metadata}")
            missing_fields = required_metadata_fields - metadata.keys()
            if missing_fields:
                raise InvalidTaskError(f"Missing required fields in metadata at index {idx}: {missing_fields}")

    def load_task_info(self, task_metadata: List[Dict[str, Any]]) -> None:

        for idx, metadata in enumerate(task_metadata, start=1):
            task_id = metadata.get('taskid')
            operator = metadata.get('operator')
            dependency_type = metadata.get('dependencytype')

            if not task_id:
                self.logger.warning(f"Skipping metadata at index {idx} without 'taskid': {metadata}")
                continue

            if task_id in self.task_info:
                raise InvalidTaskError(f"Duplicate taskid found: '{task_id}' at metadata index {idx}.")

            if not operator:
                raise InvalidTaskError(f"Operator not defined for task '{task_id}' at metadata index {idx}.")

            if not dependency_type:
                raise InvalidTaskError(f"Dependency type not defined for task '{task_id}' at metadata index {idx}.")

            self.task_info[task_id] = {
                'operator': operator,
                'dependency_type': dependency_type,
                'description': metadata.get('description'),  # Optional
                'condition': metadata.get('condition')  # Optional
            }

        self.logger.info("Task metadata successfully loaded into task_info.")

    def validate_tasks_references(self, pipeline_data: List[Dict[str, Any]]) -> None:
        """
        Ensures that all dependenttaskid references exist within the pipeline.

        Args:
            pipeline_data (List[Dict[str, Any]]): List of task dependencies.

        Raises:
            InvalidTaskError: If a task references a non-existent dependenttaskid.
        """
        all_task_ids = {task['taskid'] for task in pipeline_data if 'taskid' in task}
        for idx, task in enumerate(pipeline_data, start=1):
            parent_ids = task.get('dependenttaskid')
            if isinstance(parent_ids, list):
                for parent_id in parent_ids:
                    if parent_id and parent_id not in all_task_ids:
                        raise InvalidTaskError(
                            f"Task '{task['taskid']}' at index {idx} has invalid 'dependenttaskid': '{parent_id}'.")
            elif isinstance(parent_ids, str):
                if parent_ids and parent_ids not in all_task_ids:
                    raise InvalidTaskError(
                        f"Task '{task['taskid']}' at index {idx} has invalid 'dependenttaskid': '{parent_ids}'.")
            elif parent_ids is None:
                continue
            else:
                raise InvalidTaskError(
                    f"Invalid type for 'dependenttaskid' in task '{task['taskid']}' at index {idx}: {parent_ids}.")

    def validate_task(self, task: Dict[str, Any]) -> Tuple[str, List[str]]:
        """
        Validates a single task and extracts its task ID and parent task IDs.

        Args:
            task (Dict[str, Any]): Task dictionary.

        Returns:
            Tuple[str, List[str]]: Task ID and list of parent task IDs.

        Raises:
            InvalidTaskError: If task validation fails.
        """
        if not isinstance(task, dict):
            raise InvalidTaskError("Task must be a dictionary.")

        task_id = task.get('taskid')
        parent_ids = task.get('dependenttaskid')

        if not task_id:
            raise InvalidTaskError("Task ID is required.")

        if parent_ids is None:
            parent_ids = []
        elif isinstance(parent_ids, str):
            parent_ids = [parent_ids]
        elif isinstance(parent_ids, list):
            if not all(isinstance(pid, str) for pid in parent_ids):
                raise InvalidTaskError(
                    f"'dependenttaskid' must be a list of strings or a single string. Found: {parent_ids}")
        else:
            raise InvalidTaskError(f"Invalid type for 'dependenttaskid' in task '{task_id}': {parent_ids}")

        return task_id, parent_ids

    def is_conditional_dependency(self, task_id: str) -> bool:
        """
        Checks if the given task uses a conditional dependency type.

        Args:
            task_id (str): Task identifier.

        Returns:
            bool: True if conditional dependency, False otherwise.
        """
        dependency_type = self.task_info.get(task_id, {}).get('dependency_type')
        return dependency_type == 'conditional'

    def get_dependency_type(self, task_id: str) -> str:
        """
        Retrieves the dependency type for a given task.

        Args:
            task_id (str): Task identifier.

        Returns:
            str: Dependency type.
        """
        return self.task_info.get(task_id, {}).get('dependency_type', 'sequential')

    def detect_cycles(self, graph: Dict[str, Dict[str, List[str]]], all_tasks: Set[str]) -> List[List[str]]:
        """
        Detects cycles in the dependency graph using Depth-First Search (DFS).

        Args:
            graph (Dict[str, Dict[str, List[str]]]): Dependency graph.
            all_tasks (Set[str]): Set of all task IDs.

        Returns:
            List[List[str]]: List of cycles detected, each represented as a list of task IDs.
        """
        visited: Set[str] = set()
        path: List[str] = []
        cycles: List[List[str]] = []

        def dfs(task: str) -> None:
            if task in path:
                cycle_start = path.index(task)
                cycle = path[cycle_start:] + [task]
                cycles.append(cycle)
                return

            if task in visited:
                return

            visited.add(task)
            path.append(task)

            for dep_type, tasks in graph.get(task, {}).items():
                for next_task in tasks:
                    dfs(next_task)

            path.pop()

        for task in all_tasks:
            if task not in visited:
                dfs(task)

        return cycles

    def build_graph(
            self, tasks: List[Dict[str, Any]]
    ) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, int], Set[str]]:

        graph: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        in_degree: Dict[str, int] = defaultdict(int)
        all_tasks: Set[str] = set()

        for task in tasks:
            try:
                current_task, parent_tasks = self.validate_task(task)
                all_tasks.add(current_task)

                for parent_task in parent_tasks:
                    if parent_task and parent_task != current_task:
                        dep_type = self.get_dependency_type(current_task)
                        graph[parent_task][dep_type].append(current_task)
                        in_degree[current_task] += 1
                        all_tasks.add(parent_task)
            except InvalidTaskError as e:
                self.logger.warning(f"Skipping invalid task: {e}")

        self.logger.debug(f"Dependency graph built: {graph}")
        self.logger.debug(f"In-degree counts: {in_degree}")
        self.logger.debug(f"All tasks: {all_tasks}")

        return graph, in_degree, all_tasks

    def format_dependencies(self, task: str, dep_groups: Dict[str, List[str]]) -> List[str]:

        dependencies = []

        # Collect all downstream tasks
        downstream_tasks = []
        for tasks in dep_groups.values():
            downstream_tasks.extend(tasks)
        downstream_tasks = sorted(downstream_tasks)

        if not downstream_tasks:
            return dependencies

        # Handle conditional dependencies
        if self.is_conditional_dependency(task):
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]  # Conditional branching")
            return dependencies

        # Handle parallel dependencies
        if all(
                self.get_dependency_type(child_task) == 'parallel'
                for child_task in downstream_tasks
        ):
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]  # Parallel execution")
            return dependencies

        # Handle sequential dependencies
        if len(downstream_tasks) > 1:
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]  # Sequential dependencies")
        else:
            dependencies.append(f"{task} >> {downstream_tasks[0]}  # Sequential dependency")

        return dependencies

    def group_tasks_by_pipeline(self, pipeline_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:

        pipeline_tasks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        seen_deps: Dict[str, Set[Tuple[str, Union[str, Tuple[str, ...]]]]] = defaultdict(set)

        for task in pipeline_data:
            pipeline = task.get('pipelineid')
            current = task.get('taskid')
            parent = task.get('dependenttaskid')

            if not pipeline or not current:
                self.logger.warning(f"Task missing 'pipelineid' or 'taskid': {task}")
                continue

            # Convert list dependencies to tuple for hashing
            dep = (pipeline, current, tuple(parent) if isinstance(parent, list) else parent)
            if dep not in seen_deps[pipeline]:
                seen_deps[pipeline].add(dep)
                pipeline_tasks[pipeline].append(task)

        self.logger.debug(f"Pipelines grouped: {pipeline_tasks}")
        return pipeline_tasks

    def process_single_pipeline(self, pipeline_id: str, tasks: List[Dict[str, Any]]) -> List[str]:

        dependencies: List[str] = []

        # Build graph
        graph, in_degree, all_tasks = self.build_graph(tasks)

        # Detect cycles
        dependent_tasks = {task for task in all_tasks if in_degree.get(task, 0) > 0 or graph.get(task)}
        cycles = self.detect_cycles(graph, dependent_tasks)
        if cycles:
            cycle_strs = [' -> '.join(cycle) for cycle in cycles]
            raise CycleError(f"Cycles detected in pipeline '{pipeline_id}': {', '.join(cycle_strs)}")

        # Identify independent tasks
        independent_tasks = sorted(
            [task for task in all_tasks if in_degree.get(task, 0) == 0 and not graph.get(task)]
        )
        if independent_tasks:
            dependencies.append(f"[{', '.join(independent_tasks)}]  # Independent tasks")

        # Perform topological sort
        queue = deque([task for task in all_tasks if in_degree.get(task, 0) == 0])
        seen: Set[str] = set()

        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            downstream_groups = graph.get(current, {})
            deps = self.format_dependencies(current, downstream_groups)
            dependencies.extend(deps)

            for dep_type, tasks in downstream_groups.items():
                for task in tasks:
                    in_degree[task] -= 1
                    if in_degree[task] == 0:
                        queue.append(task)

        if len(seen) != len(all_tasks):
            raise CycleError(f"Cycle detected in pipeline '{pipeline_id}' during topological sort.")

        return dependencies

    def process_pipeline(
            self, pipeline_data: List[Dict[str, Any]], task_metadata: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:

        results: Dict[str, List[str]] = {}
        try:
            self.logger.info("Starting pipeline processing.")

            # Validate and load data
            self.validate_input_data(pipeline_data, task_metadata)
            self.load_task_info(task_metadata)
            self.validate_tasks_references(pipeline_data)

            # Group tasks by pipeline
            pipeline_tasks = self.group_tasks_by_pipeline(pipeline_data)

            if not pipeline_tasks:
                self.logger.warning("No valid pipelines found to process.")
                return results

            # Process each pipeline
            for pipeline_id, tasks in sorted(pipeline_tasks.items()):
                try:
                    self.logger.info(f"Processing pipeline: '{pipeline_id}'.")
                    dependencies = self.process_single_pipeline(pipeline_id, tasks)
                    results[pipeline_id] = dependencies
                    for dep in dependencies:
                        self.logger.info(f"Pipeline '{pipeline_id}': {dep}")
                except DependencyError as e:
                    self.logger.error(f"Error in pipeline '{pipeline_id}': {str(e)}")
                    results[pipeline_id] = [f"Error: {str(e)}"]
                except Exception as e:
                    self.logger.error(f"Unexpected error in pipeline '{pipeline_id}': {str(e)}")
                    results[pipeline_id] = [f"Error: {str(e)}"]

            return results

        except DependencyError as e:
            self.logger.error(f"Fatal dependency error processing pipelines: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Fatal error processing pipelines: {str(e)}")
            raise


def get_test_pipeline_data_and_metadata() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Simulates fetching pipeline data and task metadata from a database.

    Returns:
        Tuple containing pipeline data and task metadata lists.
    """
    # Pipeline data
    pipeline_data = [
        # Test Case 1: Empty Pipeline
        # No tasks for 'empty_pipeline' (implicitly represented by absence of tasks)

        # Test Case 2: Single Task, No Dependencies
        {"pipelineid": "single_task", "taskid": "single_task_A", "dependenttaskid": None},

        # Test Case 3: Single Task, Self-dependency
        {"pipelineid": "self_dependency", "taskid": "self_dependency_A", "dependenttaskid": "self_dependency_A"},

        # Test Case 4: Simple Linear Pipeline
        {"pipelineid": "linear_pipeline", "taskid": "linear_pipeline_A", "dependenttaskid": None},
        {"pipelineid": "linear_pipeline", "taskid": "linear_pipeline_B", "dependenttaskid": "linear_pipeline_A"},
        {"pipelineid": "linear_pipeline", "taskid": "linear_pipeline_C", "dependenttaskid": "linear_pipeline_B"},

        # Test Case 5: Multiple Independent Tasks
        {"pipelineid": "independent_tasks", "taskid": "independent_tasks_A", "dependenttaskid": None},
        {"pipelineid": "independent_tasks", "taskid": "independent_tasks_B", "dependenttaskid": None},
        {"pipelineid": "independent_tasks", "taskid": "independent_tasks_C", "dependenttaskid": None},

        # Test Case 6: Fan-out Pipeline
        {"pipelineid": "fan_out_pipeline", "taskid": "fan_out_pipeline_A", "dependenttaskid": None},
        {"pipelineid": "fan_out_pipeline", "taskid": "fan_out_pipeline_B", "dependenttaskid": "fan_out_pipeline_A"},
        {"pipelineid": "fan_out_pipeline", "taskid": "fan_out_pipeline_C", "dependenttaskid": "fan_out_pipeline_A"},
        {"pipelineid": "fan_out_pipeline", "taskid": "fan_out_pipeline_D", "dependenttaskid": "fan_out_pipeline_A"},

        # Test Case 7: Fan-in Pipeline
        {"pipelineid": "fan_in_pipeline", "taskid": "fan_in_pipeline_A", "dependenttaskid": None},
        {"pipelineid": "fan_in_pipeline", "taskid": "fan_in_pipeline_B", "dependenttaskid": None},
        {"pipelineid": "fan_in_pipeline", "taskid": "fan_in_pipeline_C", "dependenttaskid": "fan_in_pipeline_A"},
        {"pipelineid": "fan_in_pipeline", "taskid": "fan_in_pipeline_C", "dependenttaskid": "fan_in_pipeline_B"},

        # Test Case 8: Complex Pipeline with Mix of Fan-in and Fan-out
        {"pipelineid": "complex_pipeline", "taskid": "complex_pipeline_start", "dependenttaskid": None},
        {"pipelineid": "complex_pipeline", "taskid": "complex_pipeline_branch1",
         "dependenttaskid": "complex_pipeline_start"},
        {"pipelineid": "complex_pipeline", "taskid": "complex_pipeline_branch2",
         "dependenttaskid": "complex_pipeline_start"},
        {"pipelineid": "complex_pipeline", "taskid": "complex_pipeline_merge",
         "dependenttaskid": "complex_pipeline_branch1"},
        {"pipelineid": "complex_pipeline", "taskid": "complex_pipeline_merge",
         "dependenttaskid": "complex_pipeline_branch2"},
        {"pipelineid": "complex_pipeline", "taskid": "complex_pipeline_end",
         "dependenttaskid": "complex_pipeline_merge"},

        # Test Case 9: Cycle in Pipeline
        {"pipelineid": "cycle_pipeline", "taskid": "cycle_pipeline_A", "dependenttaskid": "cycle_pipeline_C"},
        {"pipelineid": "cycle_pipeline", "taskid": "cycle_pipeline_B", "dependenttaskid": "cycle_pipeline_A"},
        {"pipelineid": "cycle_pipeline", "taskid": "cycle_pipeline_C", "dependenttaskid": "cycle_pipeline_B"},

        # Test Case 10: Duplicate Dependencies
        {"pipelineid": "duplicate_dependencies", "taskid": "duplicate_dependencies_A", "dependenttaskid": None},
        {"pipelineid": "duplicate_dependencies", "taskid": "duplicate_dependencies_B",
         "dependenttaskid": "duplicate_dependencies_A"},
        {"pipelineid": "duplicate_dependencies", "taskid": "duplicate_dependencies_B",
         "dependenttaskid": "duplicate_dependencies_A"},  # Duplicate
        {"pipelineid": "duplicate_dependencies", "taskid": "duplicate_dependencies_C",
         "dependenttaskid": "duplicate_dependencies_B"},
        {"pipelineid": "duplicate_dependencies", "taskid": "duplicate_dependencies_C",
         "dependenttaskid": "duplicate_dependencies_B"},  # Duplicate

        # Test Case 12: Tasks with Missing Metadata
        {"pipelineid": "missing_metadata", "taskid": "missing_metadata_A", "dependenttaskid": None},
        {"pipelineid": "missing_metadata", "taskid": "missing_metadata_B", "dependenttaskid": "missing_metadata_A"},
        # Metadata for 'missing_metadata_B' is missing

        # Test Case 13: Another Cycle
        {"pipelineid": "another_cycle", "taskid": "another_cycle_X", "dependenttaskid": "another_cycle_Y"},
        {"pipelineid": "another_cycle", "taskid": "another_cycle_Y", "dependenttaskid": "another_cycle_Z"},
        {"pipelineid": "another_cycle", "taskid": "another_cycle_Z", "dependenttaskid": "another_cycle_X"},

        # Test Case 14: Multiple Parents (Fan-in)
        {"pipelineid": "multi_parent_pipeline", "taskid": "multi_parent_pipeline_A", "dependenttaskid": None},
        {"pipelineid": "multi_parent_pipeline", "taskid": "multi_parent_pipeline_B", "dependenttaskid": None},
        {"pipelineid": "multi_parent_pipeline", "taskid": "multi_parent_pipeline_C",
         "dependenttaskid": "multi_parent_pipeline_A"},
        {"pipelineid": "multi_parent_pipeline", "taskid": "multi_parent_pipeline_C",
         "dependenttaskid": "multi_parent_pipeline_B"},
        {"pipelineid": "multi_parent_pipeline", "taskid": "multi_parent_pipeline_D",
         "dependenttaskid": "multi_parent_pipeline_C"},

        # Test Case 15: Multiple Dependencies (Fan-in, More Complex)
        {"pipelineid": "multi_dependency_pipeline", "taskid": "multi_dependency_pipeline_start",
         "dependenttaskid": None},
        {"pipelineid": "multi_dependency_pipeline", "taskid": "multi_dependency_pipeline_middle1",
         "dependenttaskid": "multi_dependency_pipeline_start"},
        {"pipelineid": "multi_dependency_pipeline", "taskid": "multi_dependency_pipeline_middle2",
         "dependenttaskid": "multi_dependency_pipeline_start"},
        {"pipelineid": "multi_dependency_pipeline", "taskid": "multi_dependency_pipeline_end",
         "dependenttaskid": "multi_dependency_pipeline_middle1"},
        {"pipelineid": "multi_dependency_pipeline", "taskid": "multi_dependency_pipeline_end",
         "dependenttaskid": "multi_dependency_pipeline_middle2"},
    ]

    task_metadata = [
        # Test Case 2: Single Task, No Dependencies
        {"taskid": "single_task_A", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 3: Single Task, Self-dependency
        {"taskid": "self_dependency_A", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 4: Simple Linear Pipeline
        {"taskid": "linear_pipeline_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "linear_pipeline_B", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "linear_pipeline_C", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 5: Multiple Independent Tasks
        {"taskid": "independent_tasks_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "independent_tasks_B", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "independent_tasks_C", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 6: Fan-out Pipeline
        {"taskid": "fan_out_pipeline_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "fan_out_pipeline_B", "operator": "PythonOperator", "dependencytype": "parallel"},
        {"taskid": "fan_out_pipeline_C", "operator": "PythonOperator", "dependencytype": "parallel"},
        {"taskid": "fan_out_pipeline_D", "operator": "PythonOperator", "dependencytype": "parallel"},

        # Test Case 7: Fan-in Pipeline
        {"taskid": "fan_in_pipeline_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "fan_in_pipeline_B", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "fan_in_pipeline_C", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 8: Complex Pipeline with Mix of Fan-in and Fan-out
        {"taskid": "complex_pipeline_start", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "complex_pipeline_branch1", "operator": "BranchPythonOperator", "dependencytype": "conditional"},
        {"taskid": "complex_pipeline_branch2", "operator": "BranchPythonOperator", "dependencytype": "conditional"},
        {"taskid": "complex_pipeline_merge", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "complex_pipeline_end", "operator": "TriggerDagRunOperator", "dependencytype": "sequential"},

        # Test Case 9: Cycle in Pipeline
        {"taskid": "cycle_pipeline_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "cycle_pipeline_B", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "cycle_pipeline_C", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 10: Duplicate Dependencies
        {"taskid": "duplicate_dependencies_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "duplicate_dependencies_B", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "duplicate_dependencies_C", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 12: Tasks with Missing Metadata
        {"taskid": "missing_metadata_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        # "missing_metadata_B" metadata is intentionally missing

        # Test Case 13: Another Cycle
        {"taskid": "another_cycle_X", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "another_cycle_Y", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "another_cycle_Z", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 14: Multiple Parents (Fan-in)
        {"taskid": "multi_parent_pipeline_A", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "multi_parent_pipeline_B", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "multi_parent_pipeline_C", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "multi_parent_pipeline_D", "operator": "PythonOperator", "dependencytype": "sequential"},

        # Test Case 15: Multiple Dependencies (Fan-in, More Complex)
        {"taskid": "multi_dependency_pipeline_start", "operator": "PythonOperator", "dependencytype": "sequential"},
        {"taskid": "multi_dependency_pipeline_middle1", "operator": "PythonOperator", "dependencytype": "parallel"},
        {"taskid": "multi_dependency_pipeline_middle2", "operator": "PythonOperator", "dependencytype": "parallel"},
        {"taskid": "multi_dependency_pipeline_end", "operator": "PythonOperator", "dependencytype": "sequential"},
    ]

    return pipeline_data, task_metadata


def main():
    import logging

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("TaskProcessorTest")

    # Fetch pipeline data and task metadata (simulated)
    pipeline_data, task_metadata = get_test_pipeline_data_and_metadata()

    # Initialize TaskProcessor
    processor = TaskProcessor(logger=logger)

    try:
        # Process the pipelines
        results = processor.process_pipeline(pipeline_data, task_metadata)

        # Display the results
        for pipeline_id, deps in results.items():
            print(f"\nPipeline: {pipeline_id}")
            for dep in deps:
                print(dep)
    except DependencyError as e:
        print(f"Dependency error encountered: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()

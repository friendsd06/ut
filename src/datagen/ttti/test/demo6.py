from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional, Any, Union
from enum import Enum
import logging


class OperatorType(Enum):
    """Supported Airflow operator types."""
    BRANCH = "BranchPythonOperator"
    PYTHON = "PythonOperator"
    TRIGGER = "TriggerDagRunOperator"
    EMAIL = "EmailOperator"
    S3 = "S3OperatorCustom"


class DependencyType(Enum):
    """Task dependency types."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


class DependencyError(Exception):
    """Base exception for dependency-related errors."""
    pass


class CycleError(DependencyError):
    """Raised when a cycle is detected in dependencies."""
    pass


class InvalidTaskError(DependencyError):
    """Raised when task data is invalid."""
    pass


class TaskProcessor:
    """
    Processes pipelines and their tasks, handling dependencies, cycle detection,
    and formatting for execution.

    Attributes:
        task_info (Dict[str, Dict[str, Any]]): Stores metadata for each task.
        logger (logging.Logger): Logger for tracking processing steps and errors.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initializes the TaskProcessor with an optional custom logger.

        Args:
            logger (Optional[logging.Logger]): Custom logger. Defaults to the root logger.
        """
        self.task_info: Dict[str, Dict[str, Any]] = {}
        self.logger = logger or logging.getLogger(__name__)

    def validate_input_data(self, pipeline_data: List[Dict[str, Any]], task_metadata: List[Dict[str, Any]]) -> None:
        """
        Validates the structure and content of pipeline data and task metadata.

        Args:
            pipeline_data (List[Dict[str, Any]]): List of task dependencies.
            task_metadata (List[Dict[str, Any]]): List of task metadata.

        Raises:
            InvalidTaskError: If validation fails.
        """
        if not isinstance(pipeline_data, list) or not isinstance(task_metadata, list):
            raise InvalidTaskError("Pipeline data and task metadata must be lists.")

        if not pipeline_data:
            raise InvalidTaskError("Pipeline data cannot be empty.")

        required_pipeline_fields = {'pipelineid', 'taskid', 'dependenttaskid'}
        required_metadata_fields = {'taskid', 'operator_type'}

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

        # Validate operator types
        valid_operators = {op.value for op in OperatorType}
        for idx, metadata in enumerate(task_metadata, start=1):
            operator_type = metadata.get('operator_type')
            if operator_type and operator_type not in valid_operators:
                raise InvalidTaskError(f"Invalid operator type in metadata at index {idx}: '{operator_type}'.")

    def load_task_info(self, task_metadata: List[Dict[str, Any]]) -> None:
        """
        Loads task metadata into the task_info dictionary.

        Args:
            task_metadata (List[Dict[str, Any]]): List of task metadata.

        Raises:
            InvalidTaskError: If duplicate task IDs are found.
        """
        for idx, metadata in enumerate(task_metadata, start=1):
            task_id = metadata.get('taskid')
            if not task_id:
                self.logger.warning(f"Skipping metadata at index {idx} without 'taskid': {metadata}")
                continue
            if task_id in self.task_info:
                raise InvalidTaskError(f"Duplicate taskid found: '{task_id}' at metadata index {idx}.")
            self.task_info[task_id] = {
                'operator_type': metadata.get('operator_type', OperatorType.PYTHON.value),
                'dependency_type': metadata.get('dependencytype', DependencyType.SEQUENTIAL.value),
                'condition': metadata.get('condition')
            }

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
                        raise InvalidTaskError(f"Task '{task['taskid']}' at index {idx} has invalid 'dependenttaskid': '{parent_id}'.")
            elif isinstance(parent_ids, str):
                if parent_ids and parent_ids not in all_task_ids:
                    raise InvalidTaskError(f"Task '{task['taskid']}' at index {idx} has invalid 'dependenttaskid': '{parent_ids}'.")
            elif parent_ids is None:
                continue
            else:
                raise InvalidTaskError(f"Invalid type for 'dependenttaskid' in task '{task['taskid']}' at index {idx}: {parent_ids}.")

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
                raise InvalidTaskError(f"'dependenttaskid' must be a list of strings or a single string. Found: {parent_ids}")
        else:
            raise InvalidTaskError(f"Invalid type for 'dependenttaskid' in task '{task_id}': {parent_ids}")

        return task_id, parent_ids

    def is_branch_operator(self, task_id: str) -> bool:
        """
        Checks if the given task uses a branch operator.

        Args:
            task_id (str): Task identifier.

        Returns:
            bool: True if branch operator, False otherwise.
        """
        return self.task_info.get(task_id, {}).get('operator_type') == OperatorType.BRANCH.value

    def get_dependency_type(self, task_id: str) -> str:
        """
        Retrieves the dependency type for a given task.

        Args:
            task_id (str): Task identifier.

        Returns:
            str: Dependency type.
        """
        return self.task_info.get(task_id, {}).get('dependency_type', DependencyType.SEQUENTIAL.value)

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
        """
        Builds a dependency graph from the list of tasks.

        Args:
            tasks (List[Dict[str, Any]]): List of task dictionaries.

        Returns:
            Tuple[Dict[str, Dict[str, List[str]]], Dict[str, int], Set[str]]:
                - graph: Dependency graph grouped by dependency type.
                - in_degree: In-degree count for each task.
                - all_tasks: Set of all task IDs.
        """
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

        return graph, in_degree, all_tasks

    def format_dependencies(self, task: str, dep_groups: Dict[str, List[str]]) -> List[str]:
        """
        Formats dependencies based on operator and dependency types.

        Args:
            task (str): Current task ID.
            dep_groups (Dict[str, List[str]]): Downstream tasks grouped by dependency type.

        Returns:
            List[str]: Formatted dependency strings.
        """
        dependencies = []

        # Collect all downstream tasks
        downstream_tasks = []
        for tasks in dep_groups.values():
            downstream_tasks.extend(tasks)
        downstream_tasks = sorted(downstream_tasks)

        if not downstream_tasks:
            return dependencies

        # Handle branch operator
        if self.is_branch_operator(task):
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]  # Branching")
            return dependencies

        # Handle parallel tasks
        if all(
            self.get_dependency_type(child_task) == DependencyType.PARALLEL.value
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
        """
        Groups tasks by their pipeline ID.

        Args:
            pipeline_data (List[Dict[str, Any]]): List of task dependencies.

        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary mapping pipeline IDs to their respective tasks.
        """
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

        return pipeline_tasks

    def process_single_pipeline(self, pipeline_id: str, tasks: List[Dict[str, Any]]) -> List[str]:
        """
        Processes a single pipeline to determine task execution dependencies.

        Args:
            pipeline_id (str): Identifier for the pipeline.
            tasks (List[Dict[str, Any]]): List of tasks in the pipeline.

        Returns:
            List[str]: Formatted list of dependencies.

        Raises:
            CycleError: If a cycle is detected within the pipeline.
        """
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
        """
        Processes all pipelines, handling validation, grouping, and dependency resolution.

        Args:
            pipeline_data (List[Dict[str, Any]]): List of task dependencies.
            task_metadata (List[Dict[str, Any]]): List of task metadata.

        Returns:
            Dict[str, List[str]]: Dictionary mapping pipeline IDs to their respective dependency lists.

        Raises:
            DependencyError: If a fatal dependency-related error occurs.
        """
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

def main():
    # Example Test Case: Pipeline with Conditional Dependencies and Multiple Dependencies
    test_case_conditional = [
        # Main Pipeline
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_start', 'dependenttaskid': None,
         'operator_type': 'PythonOperator', 'dependencytype': 'sequential'},
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_branch', 'dependenttaskid': 'task_start',
         'operator_type': 'BranchPythonOperator', 'dependencytype': 'conditional'},

        # Branch A
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_a1', 'dependenttaskid': 'task_branch',
         'operator_type': 'PythonOperator', 'dependencytype': 'sequential'},
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_a2', 'dependenttaskid': 'task_a1',
         'operator_type': 'EmailOperator', 'dependencytype': 'parallel'},

        # Branch B
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_b1', 'dependenttaskid': 'task_branch',
         'operator_type': 'S3OperatorCustom', 'dependencytype': 'sequential'},
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_b2', 'dependenttaskid': 'task_b1',
         'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},

        # Join Point with Multiple Dependencies
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_join', 'dependenttaskid': ['task_a2', 'task_b2'],
         'operator_type': 'PythonOperator', 'dependencytype': 'sequential'},

        # Final Task
        {'pipelineid': 'pipeline_conditional', 'taskid': 'task_end', 'dependenttaskid': 'task_join',
         'operator_type': 'TriggerDagRunOperator', 'dependencytype': 'sequential'},
    ]

    # Extract task metadata and pipeline data
    task_metadata = [
        {'taskid': task['taskid'], 'operator_type': task['operator_type'], 'dependencytype': task['dependencytype']}
        for task in test_case_conditional
    ]

    pipeline_data = [
        {'pipelineid': task['pipelineid'], 'taskid': task['taskid'], 'dependenttaskid': task['dependenttaskid']}
        for task in test_case_conditional
    ]

    # Initialize TaskProcessor with default logger
    processor = TaskProcessor()

    try:
        results = processor.process_pipeline(pipeline_data, task_metadata)
        for pipeline_id, deps in results.items():
            print(f"\nPipeline: {pipeline_id}")
            for dep in deps:
                print(dep)
    except Exception as e:
        print(f"An error occurred during pipeline processing: {e}")


if __name__ == "__main__":
    main()
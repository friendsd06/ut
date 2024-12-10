from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional, Any, Union
from enum import Enum
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OperatorType(Enum):
    """Supported Airflow operator types"""
    BRANCH = "BranchPythonOperator"
    PYTHON = "PythonOperator"
    TRIGGER = "TriggerDagRunOperator"
    EMAIL = "EmailOperator"
    S3 = "S3OperatorCustom"


class DependencyType(Enum):
    """Task dependency types"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


class DependencyError(Exception):
    """Base exception for dependency related errors"""
    pass


class CycleError(DependencyError):
    """Raised when a cycle is detected in dependencies"""
    pass


class InvalidTaskError(DependencyError):
    """Raised when task data is invalid"""
    pass


class TaskProcessor:
    def __init__(self):
        self.task_info: Dict[str, Dict[str, Any]] = {}  # Store task metadata
        self.logger = logger

    def validate_input_data(self, pipeline_data: List[Dict[str, Any]], task_metadata: List[Dict[str, Any]]) -> None:
        """Validate input data structure and content"""
        if not isinstance(pipeline_data, list) or not isinstance(task_metadata, list):
            raise InvalidTaskError("Pipeline data and task metadata must be lists")

        if not pipeline_data:
            raise InvalidTaskError("Pipeline data cannot be empty")

        required_pipeline_fields = {'pipelineid', 'taskid', 'dependenttaskid'}
        required_metadata_fields = {'taskid', 'operator_type'}

        # Validate pipeline data
        for idx, task in enumerate(pipeline_data, start=1):
            if not isinstance(task, dict):
                raise InvalidTaskError(f"Invalid task format at index {idx}: {task}")
            missing_fields = required_pipeline_fields - set(task.keys())
            if missing_fields:
                raise InvalidTaskError(f"Missing required fields in task at index {idx}: {missing_fields}")

        # Validate metadata
        for idx, metadata in enumerate(task_metadata, start=1):
            if not isinstance(metadata, dict):
                raise InvalidTaskError(f"Invalid metadata format at index {idx}: {metadata}")
            missing_fields = required_metadata_fields - set(metadata.keys())
            if missing_fields:
                raise InvalidTaskError(f"Missing required fields in metadata at index {idx}: {missing_fields}")

        # Validate operator types
        valid_operators = {op.value for op in OperatorType}
        for idx, metadata in enumerate(task_metadata, start=1):
            operator_type = metadata.get('operator_type')
            if operator_type and operator_type not in valid_operators:
                raise InvalidTaskError(f"Invalid operator type in metadata at index {idx}: {operator_type}")

    def load_task_info(self, tasks: List[Dict[str, Any]]) -> None:
        """Load task information including operator types and dependency types"""
        for idx, task in enumerate(tasks, start=1):
            task_id = task.get('taskid')
            if not task_id:
                self.logger.warning(f"Skipping task at index {idx} without 'taskid': {task}")
                continue
            if task_id in self.task_info:
                raise InvalidTaskError(f"Duplicate taskid found: {task_id} at index {idx}")
            self.task_info[task_id] = {
                'operator_type': task.get('operator_type', OperatorType.PYTHON.value),
                'dependency_type': task.get('dependencytype', DependencyType.SEQUENTIAL.value),
                'condition': task.get('condition')
            }

    def validate_tasks_references(self, pipeline_data: List[Dict[str, Any]]) -> None:
        """Ensure that all dependenttaskid references are valid"""
        all_task_ids = {task['taskid'] for task in pipeline_data if 'taskid' in task}
        for idx, task in enumerate(pipeline_data, start=1):
            parent_ids = task.get('dependenttaskid')
            if isinstance(parent_ids, list):
                for parent_id in parent_ids:
                    if parent_id and parent_id not in all_task_ids:
                        raise InvalidTaskError(f"Task at index {idx} has invalid 'dependenttaskid': {parent_id}")
            elif isinstance(parent_ids, str):
                if parent_ids and parent_ids not in all_task_ids:
                    raise InvalidTaskError(f"Task at index {idx} has invalid 'dependenttaskid': {parent_ids}")
            elif parent_ids is None:
                continue
            else:
                raise InvalidTaskError(f"Invalid type for 'dependenttaskid' in task at index {idx}: {parent_ids}")

    def validate_task(self, task: Dict[str, Any]) -> Tuple[str, List[str]]:
        """Validates task data and returns task ID and list of parent task IDs"""
        if not isinstance(task, dict):
            raise InvalidTaskError("Task must be a dictionary")

        task_id = task.get('taskid')
        parent_ids = task.get('dependenttaskid')

        if not task_id:
            raise InvalidTaskError("Task ID is required")

        if parent_ids is None:
            parent_ids = []
        elif isinstance(parent_ids, str):
            parent_ids = [parent_ids]
        elif isinstance(parent_ids, list):
            pass
        else:
            raise InvalidTaskError(f"Invalid type for 'dependenttaskid' in task {task_id}")

        return task_id, parent_ids

    def is_branch_operator(self, task_id: str) -> bool:
        """Check if task uses branch operator"""
        return self.task_info.get(task_id, {}).get('operator_type') == OperatorType.BRANCH.value

    def get_dependency_type(self, task_id: str) -> str:
        """Get dependency type for a task"""
        return self.task_info.get(task_id, {}).get('dependency_type', DependencyType.SEQUENTIAL.value)

    def detect_cycles(self, graph: Dict[str, Dict[str, List[str]]], all_tasks: Set[str]) -> List[List[str]]:
        """Detect cycles using DFS and return cycle paths if found"""
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

    def build_graph(self, tasks: List[Dict[str, Any]]) -> Tuple[
        Dict[str, Dict[str, List[str]]], Dict[str, int], Set[str]]:
        """Build graph with grouped dependencies by type"""
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
        """Format dependencies based on operator and dependency types"""
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
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]")
            return dependencies

        # Handle parallel tasks
        if all(self.get_dependency_type(t) == DependencyType.PARALLEL.value for t in downstream_tasks):
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]")
            return dependencies

        # Handle sequential dependencies
        if len(downstream_tasks) > 1:
            dependencies.append(f"{task} >> [{', '.join(downstream_tasks)}]")
        else:
            dependencies.append(f"{task} >> {downstream_tasks[0]}")

        return dependencies

    def group_tasks_by_pipeline(self, pipeline_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group tasks by their pipeline ID"""
        pipeline_tasks: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        seen_deps: Dict[str, Set[Tuple[str, Union[str, List[str]]]]] = defaultdict(set)

        for task in pipeline_data:
            pipeline = task.get('pipelineid')
            current = task.get('taskid')
            parent = task.get('dependenttaskid')

            if not pipeline or not current:
                self.logger.warning(f"Task missing 'pipelineid' or 'taskid': {task}")
                continue

            dep = (pipeline, current, tuple(parent) if isinstance(parent, list) else parent)
            if dep not in seen_deps[pipeline]:
                seen_deps[pipeline].add(dep)
                pipeline_tasks[pipeline].append(task)

        return pipeline_tasks

    def process_single_pipeline(self, pipeline_id: str, tasks: List[Dict[str, Any]]) -> List[str]:
        """Process a single pipeline and return its dependencies"""
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
        independent_tasks = sorted([task for task in all_tasks if in_degree.get(task, 0) == 0 and not graph.get(task)])
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
            raise CycleError(f"Cycle detected in pipeline '{pipeline_id}' during topological sort")

        return dependencies

    def process_pipeline(self, pipeline_data: List[Dict[str, Any]], task_metadata: List[Dict[str, Any]]) -> Dict[
        str, List[str]]:
        """Process pipeline with enhanced dependency handling"""
        results: Dict[str, List[str]] = {}
        try:
            self.logger.info("Starting pipeline processing")

            # Validate and load data
            self.validate_input_data(pipeline_data, task_metadata)
            self.load_task_info(task_metadata)
            self.validate_tasks_references(pipeline_data)

            # Group tasks by pipeline
            pipeline_tasks = self.group_tasks_by_pipeline(pipeline_data)

            # Process each pipeline
            for pipeline_id, tasks in sorted(pipeline_tasks.items()):
                try:
                    self.logger.info(f"Processing pipeline: {pipeline_id}")
                    dependencies = self.process_single_pipeline(pipeline_id, tasks)
                    results[pipeline_id] = dependencies
                    for dep in dependencies:
                        self.logger.info(dep)
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


def test_all_scenarios():
    """Test all possible dependency scenarios"""

    test_cases = [
        # Case 1: Multiple Parent Dependencies
        {
            'metadata': [
                {'taskid': 'A', 'operator_type': 'PythonOperator'},
                {'taskid': 'B', 'operator_type': 'PythonOperator'},
                {'taskid': 'C', 'operator_type': 'PythonOperator'},
                {'taskid': 'D', 'operator_type': 'PythonOperator'}
            ],
            'pipeline': [
                {'pipelineid': 'multi_parent', 'taskid': 'A', 'dependenttaskid': None},
                {'pipelineid': 'multi_parent', 'taskid': 'B', 'dependenttaskid': None},
                {'pipelineid': 'multi_parent', 'taskid': 'C', 'dependenttaskid': 'A'},
                {'pipelineid': 'multi_parent', 'taskid': 'C', 'dependenttaskid': 'B'},
                {'pipelineid': 'multi_parent', 'taskid': 'D', 'dependenttaskid': 'C'}
            ]
        },

        # Case 2: Mixed Dependencies (Branch, Parallel, Sequential)
        {
            'metadata': [
                {'taskid': 'start', 'operator_type': 'PythonOperator'},
                {'taskid': 'branch', 'operator_type': 'BranchPythonOperator'},
                {'taskid': 'parallel1', 'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},
                {'taskid': 'parallel2', 'operator_type': 'PythonOperator', 'dependencytype': 'parallel'},
                {'taskid': 'sequential', 'operator_type': 'PythonOperator'},
                {'taskid': 'end', 'operator_type': 'PythonOperator'}
            ],
            'pipeline': [
                {'pipelineid': 'mixed', 'taskid': 'start', 'dependenttaskid': None},
                {'pipelineid': 'mixed', 'taskid': 'branch', 'dependenttaskid': 'start'},
                {'pipelineid': 'mixed', 'taskid': 'parallel1', 'dependenttaskid': 'branch'},
                {'pipelineid': 'mixed', 'taskid': 'parallel2', 'dependenttaskid': 'branch'},
                {'pipelineid': 'mixed', 'taskid': 'sequential', 'dependenttaskid': ['parallel1', 'parallel2']},
                {'pipelineid': 'mixed', 'taskid': 'end', 'dependenttaskid': 'sequential'}
            ]
        },

        # Case 3: Conditional Dependencies with Branch
        {
            'metadata': [
                {'taskid': 'validate', 'operator_type': 'BranchPythonOperator'},
                {'taskid': 'success_path', 'operator_type': 'PythonOperator', 'condition': 'is_valid'},
                {'taskid': 'error_path', 'operator_type': 'PythonOperator', 'condition': 'is_invalid'},
                {'taskid': 'notify', 'operator_type': 'EmailOperator'}
            ],
            'pipeline': [
                {'pipelineid': 'conditional', 'taskid': 'validate', 'dependenttaskid': None},
                {'pipelineid': 'conditional', 'taskid': 'success_path', 'dependenttaskid': 'validate'},
                {'pipelineid': 'conditional', 'taskid': 'error_path', 'dependenttaskid': 'validate'},
                {'pipelineid': 'conditional', 'taskid': 'notify', 'dependenttaskid': ['success_path', 'error_path']}
            ]
        },

        # Case 4: Empty Pipeline
        {
            'metadata': [],
            'pipeline': []
        },

        # Case 5: Invalid Dependencies
        {
            'metadata': [
                {'taskid': 'A', 'operator_type': 'PythonOperator'},
                {'taskid': 'B', 'operator_type': 'PythonOperator'}
            ],
            'pipeline': [
                {'pipelineid': 'invalid', 'taskid': 'A', 'dependenttaskid': 'nonexistent'},
                {'pipelineid': 'invalid', 'taskid': 'B', 'dependenttaskid': 'A'}
            ]
        }
    ]

    processor = TaskProcessor()

    for i, test_case in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        print("=" * 50)
        try:
            results = processor.process_pipeline(test_case['pipeline'], test_case['metadata'])
            print(f"\nResults:")
            for pipeline_id, deps in results.items():
                print(f"\nPipeline: {pipeline_id}")
                for dep in deps:
                    print(dep)
        except Exception as e:
            print(f"Error (expected for some test cases): {str(e)}")
        print("=" * 50)


if __name__ == "__main__":
    test_all_scenarios()

from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional
from enum import Enum
import logging
from datetime import datetime

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
        self.task_info = {}  # Store task metadata
        self.logger = logger

    def validate_input_data(self, pipeline_data: List[Dict], task_metadata: List[Dict]) -> None:
        """Validate input data structure and content"""
        if not isinstance(pipeline_data, list) or not isinstance(task_metadata, list):
            raise InvalidTaskError("Pipeline data and task metadata must be lists")

        if not pipeline_data:
            raise InvalidTaskError("Pipeline data cannot be empty")

        required_pipeline_fields = {'pipelineid', 'taskid', 'dependenttaskid'}
        required_metadata_fields = {'taskid', 'operator_type'}

        # Validate pipeline data
        for task in pipeline_data:
            if not isinstance(task, dict):
                raise InvalidTaskError(f"Invalid task format: {task}")
            missing_fields = required_pipeline_fields - set(task.keys())
            if missing_fields:
                raise InvalidTaskError(f"Missing required fields in task: {missing_fields}")

        # Validate task metadata
        for metadata in task_metadata:
            if not isinstance(metadata, dict):
                raise InvalidTaskError(f"Invalid metadata format: {metadata}")
            missing_fields = required_metadata_fields - set(metadata.keys())
            if missing_fields:
                raise InvalidTaskError(f"Missing required fields in metadata: {missing_fields}")

        # Validate operator types
        valid_operators = {op.value for op in OperatorType}
        for metadata in task_metadata:
            operator_type = metadata.get('operator_type')
            if operator_type and operator_type not in valid_operators:
                raise InvalidTaskError(f"Invalid operator type: {operator_type}")

    def load_task_info(self, tasks: List[Dict]) -> None:
        """Load task information including operator types and dependency types"""
        for task in tasks:
            task_id = task.get('taskid')
            if task_id:
                self.task_info[task_id] = {
                    'operator_type': task.get('operator_type', 'PythonOperator'),
                    'dependency_type': task.get('dependencytype', 'sequential'),
                    'condition': task.get('condition')
                }

    def validate_task(self, task: Dict) -> Tuple[str, Optional[str]]:
        """Validates task data and returns task ID and parent task ID"""
        if not isinstance(task, dict):
            raise InvalidTaskError("Task must be a dictionary")

        task_id = task.get('taskid')
        parent_id = task.get('dependenttaskid')

        if not task_id:
            raise InvalidTaskError("Task ID is required")

        return task_id, parent_id

    def is_branch_operator(self, task_id: str) -> bool:
        """Check if task uses branch operator"""
        return self.task_info.get(task_id, {}).get('operator_type') == OperatorType.BRANCH.value

    def validate_dependency_types(self, task_id: str, dep_type: str) -> str:
        """Validate and normalize dependency type"""
        valid_types = {dep.value for dep in DependencyType}

        if not dep_type:
            return DependencyType.SEQUENTIAL.value

        if dep_type not in valid_types:
            self.logger.warning(
                f"Invalid dependency type '{dep_type}' for task '{task_id}', "
                f"defaulting to {DependencyType.SEQUENTIAL.value}"
            )
            return DependencyType.SEQUENTIAL.value

        return dep_type

    def get_dependency_type(self, task_id: str) -> str:
        """Get validated dependency type for a task"""
        task_info = self.task_info.get(task_id, {})
        dep_type = task_info.get('dependency_type', DependencyType.SEQUENTIAL.value)
        return self.validate_dependency_types(task_id, dep_type)

    def detect_cycles(self, graph: Dict[str, Dict[str, List[str]]], all_tasks: Set[str]) -> List[str]:
        """Detect cycles using DFS and return cycle paths if found"""
        visited = set()
        path = []
        cycles = []

        def dfs(task: str) -> None:
            if task in path:
                cycle_start = path.index(task)
                cycle = path[cycle_start:] + [task]
                cycles.append(' -> '.join(cycle))
                return

            if task in visited:
                return

            visited.add(task)
            path.append(task)

            for dep_type, tasks in graph.get(task, {}).items():
                for next_task in tasks:
                    dfs(next_task)

            path.pop()

        # Check each task that hasn't been visited
        for task in all_tasks:
            if task not in visited:
                dfs(task)

        return cycles

    def build_graph(self, tasks: List[Dict]) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, int], Set[str]]:
        """Build graph with grouped dependencies by type"""
        graph = defaultdict(lambda: defaultdict(set))
        in_degree = defaultdict(int)
        all_tasks = set()

        for task in tasks:
            try:
                current_task, parent_task = self.validate_task(task)
                all_tasks.add(current_task)

                if parent_task and parent_task != current_task:
                    # Group by dependency type
                    dep_type = self.get_dependency_type(current_task)
                    graph[parent_task][dep_type].add(current_task)
                    in_degree[current_task] += 1
                    all_tasks.add(parent_task)
            except InvalidTaskError as e:
                self.logger.warning(f"Skipping invalid task: {e}")

        # Convert sets to sorted lists
        sorted_graph = {}
        for task, dep_groups in graph.items():
            sorted_graph[task] = {
                dep_type: sorted(deps)
                for dep_type, deps in dep_groups.items()
            }

        return sorted_graph, in_degree, all_tasks

    def format_dependencies(self, task: str, dep_groups: Dict[str, List[str]]) -> List[str]:
        """Format dependencies based on operator and dependency types"""
        dependencies = []

        try:
            # Handle branch operators
            if self.is_branch_operator(task):
                all_tasks = [t for tasks in dep_groups.values() for t in tasks]
                if all_tasks:
                    dependencies.append(f"{task} >> [{', '.join(sorted(all_tasks))}]")
                return dependencies

            # Handle other operators by dependency type
            for dep_type, tasks in dep_groups.items():
                if not tasks:
                    continue

                if dep_type == DependencyType.PARALLEL.value:
                    dependencies.append(f"{task} >> [{', '.join(tasks)}]")
                elif dep_type == DependencyType.CONDITIONAL.value:
                    for downstream_task in tasks:
                        condition = self.task_info.get(downstream_task, {}).get('condition')
                        if condition:
                            dependencies.append(f"{task} >> {downstream_task} # if {condition}")
                        else:
                            dependencies.append(f"{task} >> {downstream_task}")
                else:  # sequential
                    if len(tasks) > 1:
                        dependencies.append(f"{task} >> [{', '.join(tasks)}]")
                    else:
                        dependencies.append(f"{task} >> {tasks[0]}")

        except Exception as e:
            self.logger.error(f"Error formatting dependencies for task {task}: {str(e)}")
            raise

        return dependencies

    def format_pipeline_results(self, pipeline_id: str, dependencies: List[str],
                                root_tasks: List[str], errors: List[str] = None) -> Dict:
        """Format pipeline results with additional information"""
        return {
            'pipeline_id': pipeline_id,
            'root_tasks': root_tasks,
            'dependencies': dependencies,
            'errors': errors or [],
            'status': 'error' if errors else 'success',
            'task_count': len(set(task for dep in dependencies for task in dep.split(' >> ')))
        }

    def process_pipeline(self, pipeline_data: List[Dict], task_metadata: List[Dict]) -> Dict[str, Dict]:
        """Process pipeline with comprehensive error handling and validation"""
        try:
            self.logger.info("Starting pipeline processing")

            # Validate input data
            self.validate_input_data(pipeline_data, task_metadata)

            # Initialize processing
            self.load_task_info(task_metadata)

            # Group tasks by pipeline
            pipeline_tasks = defaultdict(list)
            seen_deps = defaultdict(set)
            results = {}

            # Group by pipeline
            for task in pipeline_data:
                pipeline = task.get('pipelineid')
                current = task.get('taskid')
                parent = task.get('dependenttaskid')

                if not pipeline or not current:
                    continue

                dep = (pipeline, current, parent)
                if dep not in seen_deps[pipeline]:
                    seen_deps[pipeline].add(dep)
                    pipeline_tasks[pipeline].append(task)

            # Process each pipeline
            for pipeline_id, tasks in sorted(pipeline_tasks.items()):
                try:
                    self.logger.info(f"Processing pipeline: {pipeline_id}")

                    # Build and validate graph
                    graph, in_degree, all_tasks = self.build_graph(tasks)

                    if not all_tasks:
                        results[pipeline_id] = self.format_pipeline_results(
                            pipeline_id, [], [], ["No valid tasks found"]
                        )
                        continue

                    # Check for cycles
                    cycles = self.detect_cycles(graph, all_tasks)
                    if cycles:
                        results[pipeline_id] = self.format_pipeline_results(
                            pipeline_id, [], [], [f"Cycles detected: {', '.join(cycles)}"]
                        )
                        continue

                    # Process dependencies
                    root_tasks = sorted(task for task in all_tasks if in_degree.get(task, 0) == 0)
                    if not root_tasks:
                        results[pipeline_id] = self.format_pipeline_results(
                            pipeline_id, [], [], ["No starting tasks found"]
                        )
                        continue

                    # Process dependencies using topological sort
                    dependencies = []
                    queue = deque(root_tasks)
                    seen = set()

                    while queue:
                        current = queue.popleft()
                        seen.add(current)
                        downstream_groups = graph.get(current, {})

                        if downstream_groups:
                            deps = self.format_dependencies(current, downstream_groups)
                            dependencies.extend(deps)

                            for tasks in downstream_groups.values():
                                for task in tasks:
                                    in_degree[task] -= 1
                                    if in_degree[task] == 0:
                                        queue.append(task)

                    results[pipeline_id] = self.format_pipeline_results(
                        pipeline_id, dependencies, root_tasks
                    )
                    self.logger.info(f"Successfully processed pipeline: {pipeline_id}")

                except DependencyError as e:
                    self.logger.error(f"Dependency error in pipeline {pipeline_id}: {str(e)}")
                    results[pipeline_id] = self.format_pipeline_results(
                        pipeline_id, [], [], [str(e)]
                    )
                except Exception as e:
                    self.logger.error(f"Unexpected error in pipeline {pipeline_id}: {str(e)}")
                    results[pipeline_id] = self.format_pipeline_results(
                        pipeline_id, [], [], [f"Unexpected error: {str(e)}"]
                    )

            return results

        except Exception as e:
            self.logger.error(f"Fatal error processing pipelines: {str(e)}")
            raise

from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional
from enum import Enum


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

    def get_dependency_type(self, task_id: str) -> str:
        """Get dependency type for a task"""
        return self.task_info.get(task_id, {}).get('dependency_type', 'sequential')

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
                print(f"Skipping invalid task: {e}")

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

        return dependencies

    def process_pipeline(self, pipeline_data: List[Dict], task_metadata: List[Dict]) -> Dict[str, List[str]]:
        """Process pipeline with enhanced dependency handling"""
        try:
            # Load task metadata
            self.load_task_info(task_metadata)

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
                    print(f"\n# Pipeline: {pipeline_id}")
                    graph, in_degree, all_tasks = self.build_graph(tasks)

                    if not all_tasks:
                        print(f"Warning: No valid tasks found for pipeline {pipeline_id}")
                        continue

                    root_tasks = sorted(task for task in all_tasks if in_degree.get(task, 0) == 0)
                    if not root_tasks:
                        raise CycleError(f"No starting tasks found in pipeline {pipeline_id}")

                    print(f"[{', '.join(root_tasks)}]")

                    # Process dependencies
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

                            # Process downstream tasks
                            for tasks in downstream_groups.values():
                                for task in tasks:
                                    in_degree[task] -= 1
                                    if in_degree[task] == 0:
                                        queue.append(task)

                    if len(seen) != len(all_tasks):
                        raise CycleError(f"Cycle detected in pipeline {pipeline_id}")

                    results[pipeline_id] = dependencies
                    for dep in dependencies:
                        print(dep)

                except DependencyError as e:
                    print(f"Error in pipeline {pipeline_id}: {str(e)}")
                    results[pipeline_id] = [f"Error: {str(e)}"]

            return results

        except Exception as e:
            print(f"Fatal error processing pipelines: {str(e)}")
            raise


def create_test_cases():
    """Create comprehensive test cases for branch operators and cycle detection"""

    # Test Case 1: Branch Operator with Conditions
    branch_test_metadata = [
        # Branch task with conditions
        {
            'taskid': 'validate_data',
            'operator_type': 'BranchPythonOperator',
            'dependencytype': 'conditional'
        },
        # Success path tasks
        {
            'taskid': 'process_valid_data',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential',
            'condition': 'is_valid'
        },
        {
            'taskid': 'transform_data',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        # Error path tasks
        {
            'taskid': 'handle_invalid_data',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential',
            'condition': 'is_invalid'
        },
        {
            'taskid': 'send_error_notification',
            'operator_type': 'EmailOperator',
            'dependencytype': 'sequential'
        },
    ]

    branch_test_dependencies = [
        # Initial task
        {
            'pipelineid': 'branch_test',
            'taskid': 'validate_data',
            'dependenttaskid': None
        },
        # Success path
        {
            'pipelineid': 'branch_test',
            'taskid': 'process_valid_data',
            'dependenttaskid': 'validate_data'
        },
        {
            'pipelineid': 'branch_test',
            'taskid': 'transform_data',
            'dependenttaskid': 'process_valid_data'
        },
        # Error path
        {
            'pipelineid': 'branch_test',
            'taskid': 'handle_invalid_data',
            'dependenttaskid': 'validate_data'
        },
        {
            'pipelineid': 'branch_test',
            'taskid': 'send_error_notification',
            'dependenttaskid': 'handle_invalid_data'
        }
    ]

    # Test Case 2: Multiple Branch Operators
    complex_branch_metadata = [
        # First branch
        {
            'taskid': 'check_data_type',
            'operator_type': 'BranchPythonOperator',
            'dependencytype': 'conditional'
        },
        # CSV path
        {
            'taskid': 'process_csv',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential',
            'condition': 'is_csv'
        },
        # JSON path with another branch
        {
            'taskid': 'process_json',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential',
            'condition': 'is_json'
        },
        {
            'taskid': 'validate_json_schema',
            'operator_type': 'BranchPythonOperator',
            'dependencytype': 'conditional'
        },
        {
            'taskid': 'transform_valid_json',
            'operator_type': 'PythonOperator',
            'dep endencytype': 'sequential',
            'condition': 'valid_schema'
        },
        {
            'taskid': 'handle_invalid_json',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential',
            'condition': 'invalid_schema'
        }
    ]

    complex_branch_dependencies = [
        # Initial branch
        {
            'pipelineid': 'complex_branch',
            'taskid': 'check_data_type',
            'dependenttaskid': None
        },
        # CSV path
        {
            'pipelineid': 'complex_branch',
            'taskid': 'process_csv',
            'dependenttaskid': 'check_data_type'
        },
        # JSON path
        {
            'pipelineid': 'complex_branch',
            'taskid': 'process_json',
            'dependenttaskid': 'check_data_type'
        },
        {
            'pipelineid': 'complex_branch',
            'taskid': 'validate_json_schema',
            'dependenttaskid': 'process_json'
        },
        {
            'pipelineid': 'complex_branch',
            'taskid': 'transform_valid_json',
            'dependenttaskid': 'validate_json_schema'
        },
        {
            'pipelineid': 'complex_branch',
            'taskid': 'handle_invalid_json',
            'dependenttaskid': 'validate_json_schema'
        }
    ]

    # Test Case 3: Simple Cycle
    cycle_test_metadata = [
        {
            'taskid': 'task_A',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        {
            'taskid': 'task_B',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        }
    ]

    cycle_test_dependencies = [
        {
            'pipelineid': 'cycle_test',
            'taskid': 'task_A',
            'dependenttaskid': 'task_B'
        },
        {
            'pipelineid': 'cycle_test',
            'taskid': 'task_B',
            'dependenttaskid': 'task_A'
        }
    ]

    # Test Case 4: Complex Cycle with Multiple Paths
    complex_cycle_metadata = [
        {
            'taskid': 'start',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        {
            'taskid': 'process_a',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        {
            'taskid': 'process_b',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        {
            'taskid': 'process_c',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        }
    ]

    complex_cycle_dependencies = [
        # Valid path
        {
            'pipelineid': 'complex_cycle',
            'taskid': 'process_a',
            'dependenttaskid': 'start'
        },
        # Cycle path
        {
            'pipelineid': 'complex_cycle',
            'taskid': 'process_b',
            'dependenttaskid': 'process_a'
        },
        {
            'pipelineid': 'complex_cycle',
            'taskid': 'process_c',
            'dependenttaskid': 'process_b'
        },
        {
            'pipelineid': 'complex_cycle',
            'taskid': 'process_a',
            'dependenttaskid': 'process_c'
        }
    ]

    # Test Case 5: Hidden Cycle in Branch
    branch_cycle_metadata = [
        {
            'taskid': 'start',
            'operator_type': 'BranchPythonOperator',
            'dependencytype': 'conditional'
        },
        {
            'taskid': 'branch_a',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        {
            'taskid': 'branch_b',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        },
        {
            'taskid': 'merge',
            'operator_type': 'PythonOperator',
            'dependencytype': 'sequential'
        }
    ]

    branch_cycle_dependencies = [
        {
            'pipelineid': 'branch_cycle',
            'taskid': 'branch_a',
            'dependenttaskid': 'start'
        },
        {
            'pipelineid': 'branch_cycle',
            'taskid': 'branch_b',
            'dependenttaskid': 'start'
        },
        {
            'pipelineid': 'branch_cycle',
            'taskid': 'merge',
            'dependenttaskid': 'branch_a'
        },
        {
            'pipelineid': 'branch_cycle',
            'taskid': 'branch_a',
            'dependenttaskid': 'merge'  # Creates cycle
        }
    ]

    return {
        'branch_test': (branch_test_metadata, branch_test_dependencies),
        'complex_branch': (complex_branch_metadata, complex_branch_dependencies),
        'cycle_test': (cycle_test_metadata, cycle_test_dependencies),
        'complex_cycle': (complex_cycle_metadata, complex_cycle_dependencies),
        'branch_cycle': (branch_cycle_metadata, branch_cycle_dependencies)
    }


def run_tests():
    """Run all test cases"""
    processor = TaskProcessor()
    test_cases = create_test_cases()

    for test_name, (metadata, dependencies) in test_cases.items():
        print(f"\n{'=' * 20} Testing: {test_name} {'=' * 20}")
        try:
            results = processor.process_pipeline(dependencies, metadata)
            print(f"\nResults for {test_name}:")
            for pipeline_id, deps in results.items():
                if isinstance(deps, list) and deps and deps[0].startswith('Error'):
                    print(f"Error detected: {deps[0]}")
                else:
                    print(f"\nPipeline: {pipeline_id}")
                    for dep in deps:
                        print(dep)
        except Exception as e:
            print(f"Error processing {test_name}: {str(e)}")
        print('=' * 50)


if __name__ == "__main__":
    run_tests()
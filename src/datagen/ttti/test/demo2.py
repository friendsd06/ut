from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional
from enum import Enum


class OperatorType(Enum):
    BRANCH = "BranchPythonOperator"
    PYTHON = "PythonOperator"
    TRIGGER = "TriggerDagRunOperator"
    EMAIL = "EmailOperator"
    S3 = "S3OperatorCustom"


class DependencyType(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


class TaskProcessor:
    def __init__(self):
        self.task_info = {}
        self.special_operators = {
            OperatorType.BRANCH.value,
            OperatorType.TRIGGER.value
        }

    def load_task_info(self, task_data: List[Dict]):
        """Load task information with enhanced validation"""
        for task in task_data:
            task_id = task.get('taskid')
            if task_id:
                self.task_info[task_id] = {
                    'operator_type': task.get('operator_type'),
                    'taskname': task.get('taskname'),
                    'taskgroup': task.get('taskgroup'),
                    'condition': task.get('condition'),
                    'dependencytype': task.get('dependencytype', 'sequential')
                }

    def is_special_operator(self, task_id: str) -> bool:
        """Check if task uses special operator that affects dependency format"""
        task = self.task_info.get(task_id, {})
        return task.get('operator_type') in self.special_operators

    def get_dependency_type(self, task_id: str, child_id: str) -> str:
        """Get dependency type between tasks"""
        task = self.task_info.get(task_id, {})
        return task.get('dependencytype', 'sequential')

    def build_graph(self, tasks: List[Dict]) -> Tuple[Dict[str, List[str]], Dict[str, int], Set[str]]:
        graph = defaultdict(set)
        in_degree = defaultdict(int)
        all_tasks = set()

        # First pass: collect all tasks and basic dependencies
        for task in tasks:
            current_task = task.get('taskid')
            parent_task = task.get('dependenttaskid')

            if not current_task:
                continue

            all_tasks.add(current_task)

            if parent_task and parent_task != current_task:
                graph[parent_task].add(current_task)
                in_degree[current_task] += 1
                all_tasks.add(parent_task)

        # Sort and group dependencies
        sorted_graph = {}
        for task, deps in graph.items():
            # Group dependencies by type
            dep_groups = defaultdict(list)
            for dep in sorted(deps):
                dep_type = self.get_dependency_type(task, dep)
                dep_groups[dep_type].append(dep)

            # Store grouped dependencies
            sorted_graph[task] = dep_groups

        return sorted_graph, in_degree, all_tasks

    def format_dependency(self, task: str, downstream_groups: Dict[str, List[str]]) -> List[str]:
        """Format dependency string based on operator type and dependency groups"""
        dependencies = []

        # Handle special operators
        if self.is_special_operator(task):
            # All downstream tasks are treated as conditional branches
            all_downstream = [task for group in downstream_groups.values() for task in group]
            if all_downstream:
                dependencies.append(f"{task} >> [{', '.join(sorted(all_downstream))}]")
            return dependencies

        # Handle regular operators with different dependency types
        for dep_type, tasks in downstream_groups.items():
            if not tasks:
                continue

            if dep_type == DependencyType.PARALLEL.value:
                dependencies.append(f"{task} >> [{', '.join(sorted(tasks))}]")
            elif dep_type == DependencyType.CONDITIONAL.value:
                for downstream in sorted(tasks):
                    condition = self.task_info.get(downstream, {}).get('condition', '')
                    if condition:
                        dependencies.append(f"{task} >> {downstream} # if {condition}")
                    else:
                        dependencies.append(f"{task} >> {downstream}")
            else:  # sequential
                if len(tasks) > 1:
                    dependencies.append(f"{task} >> [{', '.join(sorted(tasks))}]")
                else:
                    dependencies.append(f"{task} >> {tasks[0]}")

        return dependencies

    def process_dependencies(self, pipeline_data: List[Dict], task_data: List[Dict]) -> Dict[str, List[str]]:
        """Process pipeline dependencies with enhanced handling"""
        self.load_task_info(task_data)

        pipeline_tasks = defaultdict(list)
        seen_deps = defaultdict(set)
        results = {}

        # Group tasks by pipeline
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

                # Show independent tasks
                root_tasks = sorted(task for task in all_tasks if in_degree.get(task, 0) == 0)
                if root_tasks:
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
                        # Format dependencies based on type and operator
                        deps = self.format_dependency(current, downstream_groups)
                        dependencies.extend(deps)

                        # Process downstream tasks
                        for group in downstream_groups.values():
                            for child in group:
                                in_degree[child] -= 1
                                if in_degree[child] == 0:
                                    queue.append(child)

                has_cycle = len(seen) != len(all_tasks)
                if has_cycle:
                    print("Error: Cycle detected in pipeline")
                    results[pipeline_id] = ["Error: Cycle detected"]
                else:
                    for dep in dependencies:
                        print(dep)
                    results[pipeline_id] = dependencies

            except Exception as e:
                print(f"Error processing pipeline {pipeline_id}: {str(e)}")
                results[pipeline_id] = [f"Error: {str(e)}"]

        return results


# Test cases
if __name__ == "__main__":
    # Test Case 1: Branch Operator with Conditions
    # Test Case 1: Direct Cycle

    # Test all cases
    processor = TaskProcessor()
    print("\n=== Test Case 1: Direct Cycle ===")
    cycle_test = {
        'tasks': [
            {'taskid': 'A', 'operator_type': 'PythonOperator', 'taskname': 'Task A'},
            {'taskid': 'B', 'operator_type': 'PythonOperator', 'taskname': 'Task B'},
        ],
        'dependencies': [
            {'pipelineid': 'cycle_test', 'taskid': 'B', 'dependenttaskid': 'A'},
            {'pipelineid': 'cycle_test', 'taskid': 'A', 'dependenttaskid': 'B'}
        ]
    }
    processor.process_dependencies(cycle_test['dependencies'], cycle_test['tasks'])

    # Test Case 2: Complex Cycle
    print("\n=== Test Case 2: Complex Cycle ===")
    complex_cycle = {
        'tasks': [
            {'taskid': 'A', 'operator_type': 'PythonOperator'},
            {'taskid': 'B', 'operator_type': 'PythonOperator'},
            {'taskid': 'C', 'operator_type': 'PythonOperator'},
        ],
        'dependencies': [
            {'pipelineid': 'complex_cycle', 'taskid': 'B', 'dependenttaskid': 'A'},
            {'pipelineid': 'complex_cycle', 'taskid': 'C', 'dependenttaskid': 'B'},
            {'pipelineid': 'complex_cycle', 'taskid': 'A', 'dependenttaskid': 'C'}
        ]
    }
    processor.process_dependencies(complex_cycle['dependencies'], complex_cycle['tasks'])

    # Test Case 3: Invalid Tasks
    print("\n=== Test Case 3: Invalid Tasks ===")
    invalid_tasks = {
        'tasks': [
            {'taskid': '', 'operator_type': 'PythonOperator'},  # Empty taskid
            {'taskid': 'A'},  # Missing operator_type
            {},  # Empty task
            {'taskid': 'B', 'operator_type': 'InvalidOperator'},  # Invalid operator
        ],
        'dependencies': [
            {'pipelineid': 'invalid_test', 'taskid': 'A', 'dependenttaskid': None},
            {'pipelineid': 'invalid_test', 'taskid': 'B', 'dependenttaskid': 'A'}
        ]
    }
    processor.process_dependencies(invalid_tasks['dependencies'], invalid_tasks['tasks'])

    # Test Case 4: Complex Branch Operator
    print("\n=== Test Case 4: Complex Branch Operator ===")
    branch_test = {
        'tasks': [
            {'taskid': 'start', 'operator_type': 'PythonOperator'},
            {'taskid': 'branch', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'path_a', 'operator_type': 'PythonOperator'},
            {'taskid': 'path_b', 'operator_type': 'PythonOperator'},
            {'taskid': 'join', 'operator_type': 'PythonOperator'}
        ],
        'dependencies': [
            {'pipelineid': 'branch_test', 'taskid': 'branch', 'dependenttaskid': 'start'},
            {'pipelineid': 'branch_test', 'taskid': 'path_a', 'dependenttaskid': 'branch', 'condition': 'condition_a'},
            {'pipelineid': 'branch_test', 'taskid': 'path_b', 'dependenttaskid': 'branch', 'condition': 'condition_b'},
            {'pipelineid': 'branch_test', 'taskid': 'join', 'dependenttaskid': 'path_a'},
            {'pipelineid': 'branch_test', 'taskid': 'join', 'dependenttaskid': 'path_b'}
        ]
    }
    processor.process_dependencies(branch_test['dependencies'], branch_test['tasks'])

    # Test Case 5: Mixed Dependencies
    print("\n=== Test Case 5: Mixed Dependencies ===")
    mixed_test = {
        'tasks': [
            {'taskid': 'start', 'operator_type': 'PythonOperator'},
            {'taskid': 'parallel_1', 'operator_type': 'PythonOperator'},
            {'taskid': 'parallel_2', 'operator_type': 'PythonOperator'},
            {'taskid': 'branch', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'end_1', 'operator_type': 'PythonOperator'},
            {'taskid': 'end_2', 'operator_type': 'PythonOperator'}
        ],
        'dependencies': [
            {'pipelineid': 'mixed_test', 'taskid': 'parallel_1', 'dependenttaskid': 'start',
             'dependencytype': 'parallel'},
            {'pipelineid': 'mixed_test', 'taskid': 'parallel_2', 'dependenttaskid': 'start',
             'dependencytype': 'parallel'},
            {'pipelineid': 'mixed_test', 'taskid': 'branch', 'dependenttaskid': 'parallel_1'},
            {'pipelineid': 'mixed_test', 'taskid': 'branch', 'dependenttaskid': 'parallel_2'},
            {'pipelineid': 'mixed_test', 'taskid': 'end_1', 'dependenttaskid': 'branch'},
            {'pipelineid': 'mixed_test', 'taskid': 'end_2', 'dependenttaskid': 'branch'}
        ]
    }
    processor.process_dependencies(mixed_test['dependencies'], mixed_test['tasks'])

    # Test Case 6: Self Dependency
    print("\n=== Test Case 6: Self Dependency ===")
    self_dep_test = {
        'tasks': [
            {'taskid': 'A', 'operator_type': 'PythonOperator'}
        ],
        'dependencies': [
            {'pipelineid': 'self_dep', 'taskid': 'A', 'dependenttaskid': 'A'}
        ]
    }
    processor.process_dependencies(self_dep_test['dependencies'], self_dep_test['tasks'])

    # Test Case 7: Missing Dependencies
    print("\n=== Test Case 7: Missing Dependencies ===")
    missing_deps = {
        'tasks': [
            {'taskid': 'A', 'operator_type': 'PythonOperator'},
            {'taskid': 'B', 'operator_type': 'PythonOperator'}
        ],
        'dependencies': [
            {'pipelineid': 'missing_deps', 'taskid': 'B', 'dependenttaskid': 'non_existent'}
        ]
    }
    processor.process_dependencies(missing_deps['dependencies'], missing_deps['tasks'])

    # Test Case 8: Empty Pipeline
    print("\n=== Test Case 8: Empty Pipeline ===")
    empty_pipeline = {
        'tasks': [],
        'dependencies': []
    }
    processor.process_dependencies(empty_pipeline['dependencies'], empty_pipeline['tasks'])

    # Test Case 9: Multiple Branch Merges
    print("\n=== Test Case 9: Multiple Branch Merges ===")
    multiple_branches = {
        'tasks': [
            {'taskid': 'start', 'operator_type': 'PythonOperator'},
            {'taskid': 'branch1', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'branch2', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'path1', 'operator_type': 'PythonOperator'},
            {'taskid': 'path2', 'operator_type': 'PythonOperator'},
            {'taskid': 'path3', 'operator_type': 'PythonOperator'},
            {'taskid': 'merge', 'operator_type': 'PythonOperator'}
        ],
        'dependencies': [
            {'pipelineid': 'multi_branch', 'taskid': 'branch1', 'dependenttaskid': 'start'},
            {'pipelineid': 'multi_branch', 'taskid': 'path1', 'dependenttaskid': 'branch1'},
            {'pipelineid': 'multi_branch', 'taskid': 'branch2', 'dependenttaskid': 'branch1'},
            {'pipelineid': 'multi_branch', 'taskid': 'path2', 'dependenttaskid': 'branch2'},
            {'pipelineid': 'multi_branch', 'taskid': 'path3', 'dependenttaskid': 'branch2'},
            {'pipelineid': 'multi_branch', 'taskid': 'merge', 'dependenttaskid': 'path1'},
            {'pipelineid': 'multi_branch', 'taskid': 'merge', 'dependenttaskid': 'path2'},
            {'pipelineid': 'multi_branch', 'taskid': 'merge', 'dependenttaskid': 'path3'}
        ]
    }
    processor.process_dependencies(multiple_branches['dependencies'], multiple_branches['tasks'])

    # Test Case 10: Mixed Operator Types
    print("\n=== Test Case 10: Mixed Operator Types ===")
    mixed_operators = {
        'tasks': [
            {'taskid': 'extract', 'operator_type': 'PythonOperator'},
            {'taskid': 'validate', 'operator_type': 'BranchPythonOperator'},
            {'taskid': 'transform', 'operator_type': 'S3OperatorCustom'},
            {'taskid': 'notify', 'operator_type': 'EmailOperator'},
            {'taskid': 'trigger_next', 'operator_type': 'TriggerDagRunOperator'}
        ],
        'dependencies': [
            {'pipelineid': 'mixed_ops', 'taskid': 'validate', 'dependenttaskid': 'extract'},
            {'pipelineid': 'mixed_ops', 'taskid': 'transform', 'dependenttaskid': 'validate'},
            {'pipelineid': 'mixed_ops', 'taskid': 'notify', 'dependenttaskid': 'transform'},
            {'pipelineid': 'mixed_ops', 'taskid': 'trigger_next', 'dependenttaskid': 'notify'}
        ]
    }
    processor.process_dependencies(mixed_operators['dependencies'], mixed_operators['tasks'])

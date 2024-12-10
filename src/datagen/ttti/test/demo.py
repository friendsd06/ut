from collections import defaultdict, deque
from typing import List, Dict, Set, Tuple, Optional


class DependencyError(Exception):
    """Base exception for dependency related errors"""
    pass

class CycleError(DependencyError):
    """Raised when a cycle is detected in dependencies"""
    pass


class InvalidTaskError(DependencyError):
    """Raised when task data is invalid"""
    pass


def validate_task(task: Dict) -> Tuple[str, Optional[str]]:
    """Validates task data and returns task ID and parent task ID"""
    if not isinstance(task, dict):
        raise InvalidTaskError("Task must be a dictionary")

    task_id = task.get('taskid')
    parent_id = task.get('dependenttaskid')

    if not task_id:
        raise InvalidTaskError("Task ID is required")

    return task_id, parent_id


def build_graph(tasks: List[Dict]) -> Tuple[Dict[str, List[str]], Dict[str, int], Set[str]]:
    graph = defaultdict(set)
    in_degree = defaultdict(int)
    all_tasks = set()

    for task in tasks:
        try:
            current_task, parent_task = validate_task(task)
            all_tasks.add(current_task)

            if parent_task and parent_task != current_task:
                graph[parent_task].add(current_task)
                in_degree[current_task] += 1
                all_tasks.add(parent_task)
        except InvalidTaskError as e:
            print(f"Skipping invalid task: {e}")

    sorted_graph = {task: sorted(deps) for task, deps in graph.items()}
    return sorted_graph, in_degree, all_tasks


def find_task_order(
        graph: Dict[str, List[str]],
        in_degree: Dict[str, int],
        tasks: Set[str]
) -> Tuple[List[str], bool]:
    """
    Finds task execution order using topological sort.
    Returns (dependencies, has_cycle)
    """
    start_nodes = sorted(task for task in tasks if in_degree.get(task, 0) == 0)
    if not start_nodes:
        raise CycleError("No starting tasks found - possible cycle")

    dependencies = []
    queue = deque(start_nodes)
    seen = set()

    while queue:
        current = queue.popleft()
        seen.add(current)
        child_tasks = graph.get(current, [])

        if child_tasks:
            dep = (f"{current} >> [{', '.join(child_tasks)}]"
                   if len(child_tasks) > 1
                   else f"{current} >> {child_tasks[0]}")
            dependencies.append(dep)

            for child in child_tasks:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

    return dependencies, len(seen) != len(tasks)


def process_pipeline(data: List[Dict]) -> Dict[str, List[str]]:
    """
    Processes pipeline dependencies and returns results by pipeline.
    Returns Dict[pipeline_id, List[dependencies]]
    """
    pipeline_tasks = defaultdict(list)
    seen_deps = defaultdict(set)
    results = {}

    # Group tasks by pipeline
    for task in data:
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
            graph, in_degree, all_tasks = build_graph(tasks)

            root_tasks = sorted(task for task in all_tasks if in_degree.get(task, 0) == 0)
            if root_tasks:
                print(f"[{', '.join(root_tasks)}]")

            dependencies, has_cycle = find_task_order(graph, in_degree, all_tasks)

            if has_cycle:
                print("Error: Cycle detected in pipeline")
                results[pipeline_id] = ["Error: Cycle detected"]
            else:
                for dep in dependencies:
                    print(dep)
                results[pipeline_id] = dependencies

        except DependencyError as e:
            print(f"Error in pipeline {pipeline_id}: {str(e)}")
            results[pipeline_id] = [f"Error: {str(e)}"]
        except Exception as e:
            print(f"Unexpected error in pipeline {pipeline_id}: {str(e)}")
            results[pipeline_id] = [f"Error: {str(e)}"]

    return results


if __name__ == "__main__":
    # Test data
    # 1. ETL Pipeline with Data Quality Checks
    etl_pipeline = [
        # Source data extraction
        {'pipelineid': 'etl_pipeline', 'taskid': 'extract_mysql', 'dependenttaskid': None},
        {'pipelineid': 'etl_pipeline', 'taskid': 'extract_postgres', 'dependenttaskid': None},
        {'pipelineid': 'etl_pipeline', 'taskid': 'extract_mongodb', 'dependenttaskid': None},

        # Data validation
        {'pipelineid': 'etl_pipeline', 'taskid': None, 'dependenttaskid': 'extract_mysql'},
        {'pipelineid': 'etl_pipeline', 'taskid': 'validate_postgres_data', 'dependenttaskid': 'extract_postgres'},
        {'pipelineid': 'etl_pipeline', 'taskid': 'validate_mongodb_data', 'dependenttaskid': 'extract_mongodb'},

        # Data transformation
        {'pipelineid': 'etl_pipeline', 'taskid': 'transform_data', 'dependenttaskid': 'validate_mysql_data'},
        {'pipelineid': 'etl_pipeline', 'taskid': 'transform_data', 'dependenttaskid': 'validate_postgres_data'},
        {'pipelineid': 'etl_pipeline', 'taskid': 'transform_data', 'dependenttaskid': 'validate_mongodb_data'},

        # Load data
        {'pipelineid': 'etl_pipeline', 'taskid': 'load_warehouse', 'dependenttaskid': 'transform_data'},
        {'pipelineid': 'etl_pipeline', 'taskid': 'verify_load', 'dependenttaskid': 'load_warehouse'},
        {'pipelineid': 'etl_pipeline', 'taskid': 'send_completion_alert', 'dependenttaskid': 'verify_load'}
    ]

    # 2. ML Training Pipeline
    ml_pipeline = [
        # Data preparation
        {'pipelineid': 'ml_pipeline', 'taskid': 'fetch_training_data', 'dependenttaskid': None},
        {'pipelineid': 'ml_pipeline', 'taskid': 'fetch_validation_data', 'dependenttaskid': None},

        # Feature engineering
        {'pipelineid': 'ml_pipeline', 'taskid': 'clean_training_data', 'dependenttaskid': 'fetch_training_data'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'clean_validation_data', 'dependenttaskid': 'fetch_validation_data'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'feature_engineering', 'dependenttaskid': 'clean_training_data'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'feature_engineering', 'dependenttaskid': 'clean_validation_data'},

        # Model training
        {'pipelineid': 'ml_pipeline', 'taskid': 'train_model', 'dependenttaskid': 'feature_engineering'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'validate_model', 'dependenttaskid': 'train_model'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'generate_metrics', 'dependenttaskid': 'validate_model'},

        # Deployment
        {'pipelineid': 'ml_pipeline', 'taskid': 'save_model', 'dependenttaskid': 'generate_metrics'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'deploy_model', 'dependenttaskid': 'save_model'},
        {'pipelineid': 'ml_pipeline', 'taskid': 'monitor_deployment', 'dependenttaskid': 'deploy_model'}
    ]

    # 3. Data Backup Pipeline
    backup_pipeline = [
        # Preparation
        {'pipelineid': 'backup_pipeline', 'taskid': 'check_storage', 'dependenttaskid': None},
        {'pipelineid': 'backup_pipeline', 'taskid': 'verify_permissions', 'dependenttaskid': None},

        # Backup processes
        {'pipelineid': 'backup_pipeline', 'taskid': 'backup_databases', 'dependenttaskid': 'check_storage'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'backup_files', 'dependenttaskid': 'verify_permissions'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'backup_configs', 'dependenttaskid': 'verify_permissions'},

        # Verification
        {'pipelineid': 'backup_pipeline', 'taskid': 'verify_database_backup', 'dependenttaskid': 'backup_databases'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'verify_file_backup', 'dependenttaskid': 'backup_files'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'verify_config_backup', 'dependenttaskid': 'backup_configs'},

        # Cleanup and notification
        {'pipelineid': 'backup_pipeline', 'taskid': 'cleanup_old_backups', 'dependenttaskid': 'verify_database_backup'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'cleanup_old_backups', 'dependenttaskid': 'verify_file_backup'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'cleanup_old_backups', 'dependenttaskid': 'verify_config_backup'},
        {'pipelineid': 'backup_pipeline', 'taskid': 'send_backup_report', 'dependenttaskid': 'cleanup_old_backups'}
    ]

    # 4. Data Quality Pipeline
    quality_pipeline = [
        # Initial checks
        {'pipelineid': 'quality_pipeline', 'taskid': 'load_data_sample', 'dependenttaskid': None},
        {'pipelineid': 'quality_pipeline', 'taskid': 'load_quality_rules', 'dependenttaskid': None},

        # Quality checks
        {'pipelineid': 'quality_pipeline', 'taskid': 'check_completeness', 'dependenttaskid': 'load_data_sample'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'check_accuracy', 'dependenttaskid': 'load_data_sample'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'check_consistency', 'dependenttaskid': 'load_data_sample'},

        # Rule validation
        {'pipelineid': 'quality_pipeline', 'taskid': 'apply_business_rules', 'dependenttaskid': 'load_quality_rules'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'validate_constraints', 'dependenttaskid': 'load_quality_rules'},

        # Results processing
        {'pipelineid': 'quality_pipeline', 'taskid': 'aggregate_results', 'dependenttaskid': 'check_completeness'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'aggregate_results', 'dependenttaskid': 'check_accuracy'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'aggregate_results', 'dependenttaskid': 'check_consistency'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'aggregate_results', 'dependenttaskid': 'apply_business_rules'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'aggregate_results', 'dependenttaskid': 'validate_constraints'},

        # Reporting
        {'pipelineid': 'quality_pipeline', 'taskid': 'generate_quality_report', 'dependenttaskid': 'aggregate_results'},
        {'pipelineid': 'quality_pipeline', 'taskid': 'send_notifications', 'dependenttaskid': 'generate_quality_report'}
    ]

    # 5. Log Processing Pipeline
    log_pipeline = [
        # Log collection
        {'pipelineid': 'log_pipeline', 'taskid': 'collect_app_logs', 'dependenttaskid': None},
        {'pipelineid': 'log_pipeline', 'taskid': 'collect_system_logs', 'dependenttaskid': None},
        {'pipelineid': 'log_pipeline', 'taskid': 'collect_security_logs', 'dependenttaskid': None},

        # Parsing
        {'pipelineid': 'log_pipeline', 'taskid': 'parse_app_logs', 'dependenttaskid': 'collect_app_logs'},
        {'pipelineid': 'log_pipeline', 'taskid': 'parse_system_logs', 'dependenttaskid': 'collect_system_logs'},
        {'pipelineid': 'log_pipeline', 'taskid': 'parse_security_logs', 'dependenttaskid': 'collect_security_logs'},

        # Analysis
        {'pipelineid': 'log_pipeline', 'taskid': 'analyze_errors', 'dependenttaskid': 'parse_app_logs'},
        {'pipelineid': 'log_pipeline', 'taskid': 'analyze_performance', 'dependenttaskid': 'parse_system_logs'},
        {'pipelineid': 'log_pipeline', 'taskid': 'analyze_security_events', 'dependenttaskid': 'parse_security_logs'},

        # Alerting
        {'pipelineid': 'log_pipeline', 'taskid': 'generate_error_alerts', 'dependenttaskid': 'analyze_errors'},
        {'pipelineid': 'log_pipeline', 'taskid': 'generate_performance_alerts',
         'dependenttaskid': 'analyze_performance'},
        {'pipelineid': 'log_pipeline', 'taskid': 'generate_security_alerts',
         'dependenttaskid': 'analyze_security_events'},

        # Consolidation
        {'pipelineid': 'log_pipeline', 'taskid': 'consolidate_alerts', 'dependenttaskid': 'generate_error_alerts'},
        {'pipelineid': 'log_pipeline', 'taskid': 'consolidate_alerts',
         'dependenttaskid': 'generate_performance_alerts'},
        {'pipelineid': 'log_pipeline', 'taskid': 'consolidate_alerts', 'dependenttaskid': 'generate_security_alerts'},

        # Final actions
        {'pipelineid': 'log_pipeline', 'taskid': 'send_digest', 'dependenttaskid': 'consolidate_alerts'},
        {'pipelineid': 'log_pipeline', 'taskid': 'archive_logs', 'dependenttaskid': 'send_digest'}
    ]


    # 1. Direct Cycle (A → B → A)
    direct_cycle_pipeline = [
        {'pipelineid': 'cycle_pipeline_1', 'taskid': 'task_A', 'dependenttaskid': 'task_B'},
        {'pipelineid': 'cycle_pipeline_1', 'taskid': 'task_B', 'dependenttaskid': 'task_A'}
    ]

    # 2. Complex Cycle (A → B → C → A)
    complex_cycle_pipeline = [
        {'pipelineid': 'cycle_pipeline_2', 'taskid': 'task_A', 'dependenttaskid': 'task_C'},
        {'pipelineid': 'cycle_pipeline_2', 'taskid': 'task_B', 'dependenttaskid': 'task_A'},
        {'pipelineid': 'cycle_pipeline_2', 'taskid': 'task_C', 'dependenttaskid': 'task_B'}
    ]

    # 3. Mixed Valid and Cyclic Dependencies
    mixed_cycle_pipeline = [
        # Valid dependencies
        {'pipelineid': 'cycle_pipeline_3', 'taskid': 'start_task', 'dependenttaskid': None},
        {'pipelineid': 'cycle_pipeline_3', 'taskid': 'process_data', 'dependenttaskid': 'start_task'},
        {'pipelineid': 'cycle_pipeline_3', 'taskid': 'validate_data', 'dependenttaskid': 'process_data'},

        # Cycle in a branch
        {'pipelineid': 'cycle_pipeline_3', 'taskid': 'task_A', 'dependenttaskid': 'validate_data'},
        {'pipelineid': 'cycle_pipeline_3', 'taskid': 'task_B', 'dependenttaskid': 'task_A'},
        {'pipelineid': 'cycle_pipeline_3', 'taskid': 'task_A', 'dependenttaskid': 'task_B'}  # Creates cycle
    ]

    # 4. Multiple Cycles in Same Pipeline
    multiple_cycles_pipeline = [
        # Cycle 1
        {'pipelineid': 'cycle_pipeline_4', 'taskid': 'A1', 'dependenttaskid': 'B1'},
        {'pipelineid': 'cycle_pipeline_4', 'taskid': 'B1', 'dependenttaskid': 'A1'},

        # Cycle 2
        {'pipelineid': 'cycle_pipeline_4', 'taskid': 'X1', 'dependenttaskid': 'Y1'},
        {'pipelineid': 'cycle_pipeline_4', 'taskid': 'Y1', 'dependenttaskid': 'Z1'},
        {'pipelineid': 'cycle_pipeline_4', 'taskid': 'Z1', 'dependenttaskid': 'X1'},
    ]

    # 5. Complex Pipeline with Hidden Cycle
    hidden_cycle_pipeline = [
        # Initial tasks
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'extract_data', 'dependenttaskid': None},
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'validate_data', 'dependenttaskid': 'extract_data'},

        # Seemingly valid branch 1
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'process_branch_1', 'dependenttaskid': 'validate_data'},
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'transform_1', 'dependenttaskid': 'process_branch_1'},

        # Seemingly valid branch 2
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'process_branch_2', 'dependenttaskid': 'validate_data'},
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'transform_2', 'dependenttaskid': 'process_branch_2'},

        # Hidden cycle in merge
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'merge_results', 'dependenttaskid': 'transform_1'},
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'merge_results', 'dependenttaskid': 'transform_2'},
        {'pipelineid': 'cycle_pipeline_5', 'taskid': 'transform_1', 'dependenttaskid': 'merge_results'}  # Creates cycle
    ]

    # Combine all test datasets
    test_datasets = [
        etl_pipeline,
        ml_pipeline,
        backup_pipeline,
        quality_pipeline,
        log_pipeline,
        direct_cycle_pipeline,
        complex_cycle_pipeline,
        mixed_cycle_pipeline,
        multiple_cycles_pipeline,
        hidden_cycle_pipeline
    ]

    for dataset in test_datasets:
        process_pipeline(dataset)

from typing import List, Dict, Any
from .models import DAGConfig, TaskConfig, DependencyConfig
from .exceptions import ConfigurationError

class ContextBuilder:
    @staticmethod
    def build(
            dag: DAGConfig,
            tasks: List[TaskConfig],
            dependencies: List[DependencyConfig]
    ) -> Dict[str, Any]:
        return {
            "dag": dag,
            "tasks": tasks,
            "dependencies": dependencies,
            "imports": ContextBuilder._collect_imports(tasks)
        }

    @staticmethod
    def _collect_imports(tasks: List[TaskConfig]) -> Dict[str, str]:
        imports = {}
        for task in tasks:
            operator_name = task.type.value
            module_name = f"airflow.operators.{task.type.name.lower()}"
            imports[operator_name] = module_name
        return imports
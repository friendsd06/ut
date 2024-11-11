from typing import Protocol, List
from ..domain.models import DAGConfig, TaskConfig, DependencyConfig


class ConfigurationRepository(Protocol):
    """Port for accessing DAG configurations."""

    def load_dag(self, dag_id: str) -> DAGConfig: ...

    def load_tasks(self, dag_id: str) -> List[TaskConfig]: ...

    def load_dependencies(self, dag_id: str) -> List[DependencyConfig]: ...

    def validate_dag_exists(self, dag_id: str) -> bool: ...


class TemplateService(Protocol):
    """Port for template operations."""

    def render(self, template: str, context: dict) -> str: ...

    def validate_template(self, template: str) -> List[str]: ...


class StorageService(Protocol):
    """Port for storage operations."""

    def save(self, path: str, content: str) -> None: ...

    def exists(self, path: str) -> bool: ...

    def backup(self, path: str) -> None: ...

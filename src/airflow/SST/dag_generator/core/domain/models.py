from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum
import yaml


class TaskType(Enum):
    """Supported task operator types."""
    PYTHON = "PythonOperator"
    BASH = "BashOperator"
    SQL = "SqlOperator"


class DependencyType(Enum):
    """Types of task dependencies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


@dataclass(frozen=True)
class DAGConfig:
    """Represents an Airflow DAG configuration."""
    id: str
    description: str
    schedule: str
    start_date: datetime
    tags: List[str]
    settings: Dict[str, Any]

    def validate(self) -> List[str]:
        errors = []
        if not self.id:
            errors.append("DAG ID is required")
        if not self.schedule:
            errors.append("Schedule is required")
        if not self.start_date:
            errors.append("Start date is required")
        return errors


@dataclass(frozen=True)
class TaskConfig:
    """Represents a single task within a DAG."""
    id: str
    type: TaskType
    config: Dict[str, Any]
    group: Optional[str] = None
    retries: int = 0
    retry_delay: int = 300
    timeout: Optional[int] = None

    def validate(self) -> List[str]:
        errors = []
        if not self.id:
            errors.append("Task ID is required")
        if not isinstance(self.type, TaskType):
            errors.append(f"Invalid task type: {self.type}")
        return errors


@dataclass(frozen=True)
class DependencyConfig:
    """Represents task dependencies."""
    type: DependencyType
    from_tasks: List[str]
    to_tasks: List[str]

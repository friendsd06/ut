from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional
from enum import Enum

class TaskType(Enum):
    PYTHON = "PythonOperator"
    BASH = "BashOperator"
    SQL = "SqlOperator"
    HTTP = "SimpleHttpOperator"
    SENSOR = "ExternalTaskSensor"
    DOCKER = "DockerOperator"

class DependencyType(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    CROSS_DAG = "cross_dag"

@dataclass(frozen=True)
class DAGConfig:
    id: str
    description: str
    schedule: str
    start_date: datetime
    tags: List[str]
    settings: Dict[str, Any]
    owner: str = "airflow"
    email: Optional[List[str]] = None

    def validate(self) -> List[str]:
        errors = []
        if not self.id:
            errors.append("DAG ID is required")
        if not self.schedule:
            errors.append("Schedule is required")
        if not self.start_date:
            errors.append("Start date is required")
        if self.email and not all('@' in email for email in self.email):
            errors.append("Invalid email format")
        return errors

@dataclass(frozen=True)
class TaskConfig:
    id: str
    type: TaskType
    config: Dict[str, Any]
    group: Optional[str] = None
    retries: int = 0
    retry_delay: int = 300
    timeout: Optional[int] = None
    queue: Optional[str] = None
    pool: Optional[str] = None

    def validate(self) -> List[str]:
        errors = []
        if not self.id:
            errors.append("Task ID is required")
        if not isinstance(self.type, TaskType):
            errors.append(f"Invalid task type: {self.type}")
        if self.retries < 0:
            errors.append("Retries cannot be negative")
        return errors

@dataclass(frozen=True)
class DependencyConfig:
    type: DependencyType
    from_tasks: List[str]
    to_tasks: List[str]
    condition: Optional[str] = None
    external_dag_id: Optional[str] = None

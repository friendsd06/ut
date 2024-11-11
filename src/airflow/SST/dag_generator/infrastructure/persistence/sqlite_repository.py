from contextlib import contextmanager
import sqlite3
from datetime import datetime
import yaml
from typing import List, Dict, Any
from ...core.domain.models import DAGConfig, TaskConfig, DependencyConfig, TaskType, DependencyType
from ...core.ports.interfaces import ConfigurationRepository
from ...core.domain.exceptions import ConfigurationError


class SQLiteRepository(ConfigurationRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def _connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.row_factory = sqlite3.Row
            yield conn
        finally:
            conn.close()

    def _parse_yaml(self, yaml_str: str) -> Dict[str, Any]:
        if not yaml_str:
            return {}
        return yaml.safe_load(yaml_str)

    def validate_dag_exists(self, dag_id: str) -> bool:
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM dags WHERE id = ?",
                (dag_id,)
            )
            return cursor.fetchone() is not None

    def load_dag(self, dag_id: str) -> DAGConfig:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM dags WHERE id = ?",
                (dag_id,)
            ).fetchone()

            if not row:
                raise ConfigurationError(f"DAG {dag_id} not found")

            config = DAGConfig(
                id=row['id'],
                description=row['description'],
                schedule=row['schedule'],
                start_date=datetime.fromisoformat(row['start_date']),
                tags=row['tags'].split(',') if row['tags'] else [],
                settings=self._parse_yaml(row['settings'])
            )

            errors = config.validate()
            if errors:
                raise ConfigurationError(f"Invalid DAG configuration: {errors}")

            return config

    def load_tasks(self, dag_id: str) -> List[TaskConfig]:
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE dag_id = ?",
                (dag_id,)
            ).fetchall()

            return [
                TaskConfig(
                    id=row['id'],
                    type=TaskType[row['type'].upper().replace('OPERATOR', '')],
                    config=self._parse_yaml(row['config']),
                    group=row['group_id']
                )
                for row in rows
            ]

    def load_dependencies(self, dag_id: str) -> List[DependencyConfig]:
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM dependencies WHERE dag_id = ?",
                (dag_id,)
            ).fetchall()

            return [
                DependencyConfig(
                    type=DependencyType[row['type'].upper()],
                    from_tasks=row['from_tasks'].split(','),
                    to_tasks=row['to_tasks'].split(',')
                )
                for row in rows
            ]
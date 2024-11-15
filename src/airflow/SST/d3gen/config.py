from dataclasses import dataclass
from typing import Dict, Any
import os

@dataclass
class PostgresConfig:
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = int(os.getenv('POSTGRES_PORT', '5432'))
    database: str = os.getenv('POSTGRES_DB', 'airflow_dag_generator')
    user: str = os.getenv('POSTGRES_USER', 'postgres')
    password: str = os.getenv('POSTGRES_PASSWORD', 'postgres')

    def as_dict(self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }

@dataclass
class AppConfig:
    template_dir: str = os.getenv('TEMPLATE_DIR', 'examples/templates')
    output_dir: str = os.getenv('OUTPUT_DIR', 'generated_dags')
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
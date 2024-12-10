from typing import Dict, Any, Optional, List
from db_client import DBClient
from interfaces import DagDataFetcher


class DatabaseDagFetcher(DagDataFetcher):
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config

    def get_dag_metadata(self, dag_id: str) -> Dict[str, Any]:
        with DBClient(self.db_config) as db:
            return db.get_dag_metadata(dag_id)

    def get_active_dags(self, environment: Optional[str] = None) -> List[str]:
        with DBClient(self.db_config) as db:
            return db.get_active_dags(environment)

import traceback
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from queries import DAGQueries
from logger_config import get_logger


class DBClient:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = dict(db_config)  # Make a copy of config
        self.logger = get_logger(__name__)
        # Mask password in config for logging
        self.log_safe_config = {
            **self.db_config,
            'password': '***' if 'password' in self.db_config else None
        }

    def __enter__(self):
        try:
            self.logger.debug(
                f"Establishing database connection with config: {self.log_safe_config}"
            )
            self.conn = psycopg2.connect(**self.db_config)
            self.logger.info(
                f"Successfully established database connection to "
                f"{self.db_config.get('host')}:{self.db_config.get('port')}"
            )
            return self
        except Exception as e:
            self.logger.error(
                f"Failed to establish database connection. "
                f"Host: {self.db_config.get('host')}. "
                f"Port: {self.db_config.get('port')}. "
                f"Database: {self.db_config.get('database')}. "
                f"User: {self.db_config.get('user')}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            try:
                self.conn.close()
                self.logger.debug("Database connection closed successfully")
            except Exception as e:
                self.logger.error(
                    f"Error closing database connection: {str(e)}. "
                    f"Traceback: {traceback.format_exc()}"
                )

    def _execute_query(self, cursor: RealDictCursor, query: str, params: tuple) -> None:
        """Execute a query with logging."""
        try:
            self.logger.debug(
                f"Executing query: {query} "
                f"with parameters: {params}"
            )
            cursor.execute(query, params)
            self.logger.debug("Query executed successfully")
        except Exception as e:
            self.logger.error(
                f"Query execution failed. "
                f"Query: {query}. "
                f"Parameters: {params}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def get_dag_metadata(self, dag_id: str) -> Dict[str, Any]:
        """Fetch complete metadata for a DAG with detailed logging."""
        try:
            self.logger.info(f"Fetching metadata for DAG: {dag_id}")

            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get DAG configuration
                self.logger.debug(f"Fetching DAG configuration for: {dag_id}")
                self._execute_query(cur, DAGQueries.GET_DAG, (dag_id,))
                dag_config = cur.fetchone()

                if not dag_config:
                    self.logger.error(f"DAG not found: {dag_id}")
                    raise ValueError(f"DAG {dag_id} not found")

                self.logger.debug(f"Successfully fetched DAG config for: {dag_id}")

                # Get tasks
                self.logger.debug(f"Fetching tasks for DAG: {dag_id}")
                self._execute_query(cur, DAGQueries.GET_TASKS, (dag_id,))
                tasks = cur.fetchall()
                self.logger.debug(
                    f"Successfully fetched {len(tasks)} tasks for DAG: {dag_id}"
                )

                # Get task groups
                self.logger.debug(f"Fetching task groups for DAG: {dag_id}")
                self._execute_query(cur, DAGQueries.GET_TASK_GROUPS, (dag_id,))
                task_groups = cur.fetchall()
                self.logger.debug(
                    f"Successfully fetched {len(task_groups)} task groups for DAG: {dag_id}"
                )

                # Get dependencies
                self.logger.debug(f"Fetching dependencies for DAG: {dag_id}")
                self._execute_query(cur, DAGQueries.GET_DEPENDENCIES, (dag_id,))
                dependencies = cur.fetchall()
                self.logger.debug(
                    f"Successfully fetched {len(dependencies)} dependencies for DAG: {dag_id}"
                )

                metadata = {
                    'dag_config': dict(dag_config),
                    'tasks': [dict(task) for task in tasks],
                    'task_groups': [dict(group) for group in task_groups],
                    'dependencies': [dict(dep) for dep in dependencies],
                }

                self.logger.info(
                    f"Successfully fetched all metadata for DAG: {dag_id}. "
                    f"Tasks: {len(tasks)}, "
                    f"Groups: {len(task_groups)}, "
                    f"Dependencies: {len(dependencies)}"
                )

                return metadata

        except Exception as e:
            self.logger.error(
                f"Error fetching metadata for DAG {dag_id}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def get_active_dags(self, environment: Optional[str] = None) -> List[str]:
        """Get list of active DAG IDs with detailed logging."""
        try:
            self.logger.info(
                f"Fetching active DAGs for environment: "
                f"{environment if environment else 'all'}"
            )

            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                self._execute_query(
                    cur,
                    DAGQueries.GET_ACTIVE_DAGS,
                    (environment, environment)
                )
                result = cur.fetchall()
                dag_ids = [row['dag_id'] for row in result]

                self.logger.info(
                    f"Successfully fetched {len(dag_ids)} active DAGs"
                )
                self.logger.debug(f"Active DAG IDs: {dag_ids}")

                return dag_ids

        except Exception as e:
            self.logger.error(
                f"Error fetching active DAGs. "
                f"Environment: {environment}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

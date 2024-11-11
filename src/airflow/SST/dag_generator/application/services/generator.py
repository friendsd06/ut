import logging
from typing import Protocol, Optional, List, Dict
from ...core.domain.models import DAGConfig, TaskConfig, DependencyConfig
from ...core.domain.exceptions import DAGGeneratorError, ConfigurationError
from ...core.ports.interfaces import ConfigurationRepository, TemplateService, StorageService


class Logger(Protocol):
    def info(self, msg: str) -> None: ...

    def error(self, msg: str) -> None: ...


class DAGGenerator:
    def __init__(
            self,
            config_repo: ConfigurationRepository,
            template_svc: TemplateService,
            storage_svc: StorageService,
            output_dir: str,
            logger: Optional[Logger] = None
    ):
        self.config_repo = config_repo
        self.template_svc = template_svc
        self.storage_svc = storage_svc
        self.output_dir = output_dir
        self.logger = logger or logging.getLogger(__name__)

    def generate(self, dag_id: str) -> None:
        try:
            if not self.config_repo.validate_dag_exists(dag_id):
                raise ConfigurationError(f"DAG {dag_id} not found")

            # Load configurations
            dag = self.config_repo.load_dag(dag_id)
            tasks = self.config_repo.load_tasks(dag_id)
            dependencies = self.config_repo.load_dependencies(dag_id)

            # Build context
            context = {
                "dag": dag,
                "tasks": tasks,
                "dependencies": dependencies,
                "imports": self._collect_imports(tasks)
            }

            # Generate content
            content = self.template_svc.render("dag.py.j2", context)

            # Save with backup
            output_path = f"{self.output_dir}/{dag_id}.py"
            if self.storage_svc.exists(output_path):
                self.storage_svc.backup(output_path)

            self.storage_svc.save(output_path, content)
            self.logger.info(f"Successfully generated DAG: {dag_id}")

        except DAGGeneratorError as e:
            self.logger.error(f"Failed to generate DAG {dag_id}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error generating DAG {dag_id}: {str(e)}")
            raise DAGGeneratorError(f"Failed to generate DAG: {str(e)}")

    def _collect_imports(self, tasks: List[TaskConfig]) -> Dict[str, str]:
        imports = {}
        for task in tasks:
            operator_name = task.type.value
            module_name = f"airflow.operators.{task.type.name.lower()}"
            imports[operator_name] = module_name
        return imports
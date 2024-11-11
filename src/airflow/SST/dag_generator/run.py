# run.py
import logging
from dag_generator.infrastructure.persistence.sqlite_repository import SQLiteRepository
from dag_generator.infrastructure.templating.jinja_engine import JinjaEngine
from dag_generator.infrastructure.storage.local_storage import LocalStorage
from dag_generator.application.services.generator import DAGGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    project_dir = r"D:\Airflow-DAG\dag_generator"

    generator = DAGGenerator(
        config_repo=SQLiteRepository(f"{project_dir}\\dags.db"),
        template_svc=JinjaEngine(f"{project_dir}\\examples\\templates"),
        storage_svc=LocalStorage(),
        output_dir=f"{project_dir}\\generated_dags",
        logger=logger
    )

    generator.generate("example_dag")


if __name__ == "__main__":
    main()
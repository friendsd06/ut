import traceback
from pathlib import Path
from typing import Optional
from interfaces import DagDataFetcher, TemplateEngine, CodeFormatter
from logger_config import get_logger


class DagFileWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.logger = get_logger(__name__)
        self._create_output_directory()

    def _create_output_directory(self) -> None:
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(
                f"Output directory ensured at: {self.output_dir.absolute()}"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to create output directory {self.output_dir}. "
                f"Error: {str(e)}. Traceback: {traceback.format_exc()}"
            )
            raise

    def write_dag_file(self, dag_id: str, content: str) -> None:
        output_path = self.output_dir / f"{dag_id}.py"
        try:
            self.logger.debug(f"Starting to write DAG file: {output_path}")
            output_path.write_text(content)
            self.logger.info(
                f"Successfully generated DAG: {dag_id} at {output_path.absolute()}"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to write DAG file {dag_id}. "
                f"Path: {output_path}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise


class DagGenerator:
    def __init__(
            self,
            data_fetcher: DagDataFetcher,
            template_engine: TemplateEngine,
            code_formatter: CodeFormatter,
            file_writer: DagFileWriter
    ):
        self.data_fetcher = data_fetcher
        self.template_engine = template_engine
        self.code_formatter = code_formatter
        self.file_writer = file_writer
        self.logger = get_logger(__name__)

    def generate_single_dag(self, dag_id: str) -> None:
        try:
            self.logger.info(f"Starting DAG generation for: {dag_id}")

            # Fetch metadata
            self.logger.debug(f"Fetching metadata for DAG: {dag_id}")
            metadata = self.data_fetcher.get_dag_metadata(dag_id)
            self.logger.debug(
                f"Successfully fetched metadata for DAG: {dag_id}"
            )

            # Generate code from template
            self.logger.debug(f"Rendering template for DAG: {dag_id}")
            dag_code = self.template_engine.render_template(
                "dag_template.j2",
                {"metadata": metadata}
            )
            self.logger.debug(f"Successfully rendered template for DAG: {dag_id}")

            # Format code
            self.logger.debug(f"Formatting code for DAG: {dag_id}")
            formatted_code = self.code_formatter.format_code(dag_code)
            self.logger.debug(f"Successfully formatted code for DAG: {dag_id}")

            # Write to file
            self.logger.debug(f"Writing file for DAG: {dag_id}")
            self.file_writer.write_dag_file(dag_id, formatted_code)
            self.logger.info(
                f"Successfully completed DAG generation for: {dag_id}"
            )

        except Exception as e:
            self.logger.error(
                f"Error generating DAG {dag_id}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def generate_all_dags(self, environment: Optional[str] = None) -> None:
        try:
            self.logger.info(
                f"Starting bulk DAG generation for environment: "
                f"{environment if environment else 'all'}"
            )

            # Fetch active DAGs
            self.logger.debug("Fetching list of active DAGs")
            dag_ids = self.data_fetcher.get_active_dags(environment)
            self.logger.info(f"Found {len(dag_ids)} active DAGs to generate")

            # Generate each DAG
            successful_dags = []
            failed_dags = []

            for dag_id in dag_ids:
                try:
                    self.generate_single_dag(dag_id)
                    successful_dags.append(dag_id)
                except Exception as e:
                    failed_dags.append(dag_id)
                    self.logger.error(
                        f"Failed to generate DAG {dag_id}. "
                        f"Error: {str(e)}. "
                        f"Traceback: {traceback.format_exc()}"
                    )
                    continue

            # Log summary
            self.logger.info(
                f"DAG generation completed. "
                f"Success: {len(successful_dags)}, "
                f"Failed: {len(failed_dags)}"
            )

            if successful_dags:
                self.logger.info(
                    f"Successfully generated DAGs: {successful_dags}"
                )
            if failed_dags:
                self.logger.error(f"Failed to generate DAGs: {failed_dags}")

        except Exception as e:
            self.logger.error(
                f"Critical error during bulk DAG generation. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

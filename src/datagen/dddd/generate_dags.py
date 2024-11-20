# generate_dags.py
import logging
from pathlib import Path
from config import db_config
from dag_generator import DagGenerator, DagFileWriter
from data_fetcher import DatabaseDagFetcher
from formatters import BlackCodeFormatter
from template_engine import JinjaTemplateEngine

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    template_dir = Path('templates')  # Adjust the path as per your project structure
    output_dir = Path('generated_dags')  # Output directory for generated DAGs

    try:
        # Initialize components
        template_dir = Path("templates")
        output_dir = Path("dags")

        data_fetcher = DatabaseDagFetcher(db_config)
        template_engine = JinjaTemplateEngine(template_dir)
        code_formatter = BlackCodeFormatter()
        file_writer = DagFileWriter(output_dir)

        # Initialize generator
        generator = DagGenerator(
            data_fetcher=data_fetcher,
            template_engine=template_engine,
            code_formatter=code_formatter,
            file_writer=file_writer
        )

        # Generate DAGs
        generator.generate_all_dags()

    except Exception as e:
        logging.error(f"DAG generation failed: {e}")
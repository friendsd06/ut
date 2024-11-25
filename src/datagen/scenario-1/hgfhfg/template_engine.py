import traceback
from typing import Dict, Any
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime, timedelta
from pytz import timezone
import json
from logger_config import get_logger
from interfaces import TemplateEngine


class JinjaTemplateEngine(TemplateEngine):
    def __init__(self, template_dir: Path):
        self.template_dir = template_dir
        self.env = None
        self.logger = get_logger(__name__)
        self.logger.info(
            f"Initializing Jinja template engine with template directory: {template_dir}"
        )

    def initialize(self) -> None:
        """Initialize the Jinja environment with custom filters and globals."""
        try:
            self.logger.debug("Starting Jinja environment initialization")

            if not self.template_dir.exists():
                error_msg = f"Template directory not found: {self.template_dir}"
                self.logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            self.env = Environment(
                loader=FileSystemLoader(str(self.template_dir)),
                autoescape=select_autoescape(["html", "xml"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )

            self._add_filters()
            self._add_globals()

            self.logger.info("Successfully initialized Jinja environment")

        except Exception as e:
            self.logger.error(
                f"Failed to initialize Jinja environment. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def _add_filters(self) -> None:
        """Add custom filters to Jinja environment."""
        try:
            self.logger.debug("Adding custom filters to Jinja environment")

            # JSON filter
            self.env.filters["tojson"] = lambda v: json.dumps(v)

            # Datetime filter
            def datetime_filter(v):
                try:
                    return v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime) else v
                except Exception as e:
                    self.logger.error(f"Error in datetime filter: {e}")
                    return str(v)

            self.env.filters["to_datetime"] = datetime_filter

            # Datetime to code filter
            def convert_datetime_to_code(dt):
                try:
                    if isinstance(dt, datetime):
                        return f"datetime({dt.year}, {dt.month}, {dt.day}, {dt.hour}, {dt.minute}, {dt.second})"
                    return repr(dt)
                except Exception as e:
                    self.logger.error(f"Error converting datetime to code: {e}")
                    return repr(dt)

            # Timedelta to code filter
            def convert_timedelta_to_code(value):
                if isinstance(value, int):  # Assume value is in minutes
                    return f"timedelta(minutes={value})"
                elif isinstance(value, timedelta):
                    return f"timedelta(days={value.days}, seconds={value.seconds})"
                raise ValueError("Value must be an int (minutes) or timedelta object")

            self.env.filters["datetime_to_code"] = convert_datetime_to_code
            self.env.filters["timedelta_to_code"] = convert_timedelta_to_code

            self.logger.debug("Successfully added all custom filters")

        except Exception as e:
            self.logger.error(
                f"Failed to add custom filters. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def _add_globals(self) -> None:
        """Add global functions to Jinja environment."""
        try:
            self.logger.debug("Adding global functions to Jinja environment")

            def get_current_time(tz_name="UTC", fmt="%Y-%m-%d %H:%M:%S"):
                try:
                    tz = timezone(tz_name)
                    return datetime.now(tz).strftime(fmt)
                except Exception as e:
                    self.logger.error(
                        f"Error getting current time for timezone {tz_name}: {e}"
                    )
                    return datetime.utcnow().strftime(fmt)

            self.env.globals["now"] = get_current_time

            self.logger.debug("Successfully added global functions")

        except Exception as e:
            self.logger.error(
                f"Failed to add global functions. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

    def render_template(self, template_name: str, data: Dict[str, Any]) -> str:
        """Render a template with the provided data."""
        try:
            self.logger.info(f"Starting template rendering: {template_name}")
            self.logger.debug(f"Template data keys: {list(data.keys())}")

            if not self.env:
                self.logger.debug("Initializing Jinja environment")
                self.initialize()

            # Verify template exists
            try:
                template = self.env.get_template(template_name)
            except Exception as e:
                self.logger.error(
                    f"Template not found: {template_name}. "
                    f"Error: {str(e)}"
                )
                raise

            # Render template
            rendered_content = template.render(**data)

            content_length = len(rendered_content)
            self.logger.info(
                f"Successfully rendered template {template_name}. "
                f"Output length: {content_length} characters"
            )

            if content_length == 0:
                self.logger.warning(
                    f"Template {template_name} rendered empty content"
                )

            return rendered_content

        except Exception as e:
            self.logger.error(
                f"Failed to render template {template_name}. "
                f"Error: {str(e)}. "
                f"Traceback: {traceback.format_exc()}"
            )
            raise

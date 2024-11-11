from typing import List

from jinja2 import Environment, FileSystemLoader
from ...core.ports.interfaces import TemplateService


class JinjaEngine(TemplateService):
    def __init__(self, template_dir: str):
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )

    def render(self, template: str, context: dict) -> str:
        template = self.env.get_template(template)
        return template.render(**context)

    def validate_template(self, template: str) -> List[str]:
        try:
            self.env.get_template(template)
            return []
        except Exception as e:
            return [str(e)]

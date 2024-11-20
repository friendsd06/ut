from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List


class CodeFormatter(ABC):
    @abstractmethod
    def format_code(self, code: str) -> str:
        pass


class TemplateEngine(ABC):
    @abstractmethod
    def render_template(self, template_name: str, data: Dict[str, Any]) -> str:
        pass

    @abstractmethod
    def initialize(self) -> None:
        pass


class DagDataFetcher(ABC):
    @abstractmethod
    def get_dag_metadata(self, dag_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_active_dags(self, environment: Optional[str] = None) -> List[str]:
        pass
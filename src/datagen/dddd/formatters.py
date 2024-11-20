import black
import logging
from interfaces import CodeFormatter


class BlackCodeFormatter(CodeFormatter):
    def format_code(self, code: str) -> str:
        try:
            return black.format_str(code, mode=black.Mode())
        except Exception as e:
            logging.warning(f"Code formatting failed: {e}")
            return code

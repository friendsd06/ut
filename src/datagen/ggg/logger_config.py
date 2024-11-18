import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional


class LoggerConfig:
    _instance: Optional['LoggerConfig'] = None

    def __init__(self):
        self.log_format = (
            '%(asctime)s - %(name)s - %(levelname)s - [%(process)d] - '
            '%(filename)s:%(lineno)d - %(funcName)s - %(message)s'
        )
        self.date_format = '%Y-%m-%d %H:%M:%S'

        # Create logs directory if it doesn't exist
        self.log_dir = Path('logs')
        self.log_dir.mkdir(exist_ok=True)

        self.log_file = self.log_dir / 'dag_generator.log'
        self._setup_logging()

    @classmethod
    def get_instance(cls) -> 'LoggerConfig':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _setup_logging(self) -> None:
        """Configure logging with both file and console handlers."""
        # Create formatters
        formatter = logging.Formatter(
            fmt=self.log_format,
            datefmt=self.date_format
        )

        # File handler with rotation
        file_handler = RotatingFileHandler(
            filename=self.log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        # Remove existing handlers to avoid duplication
        root_logger.handlers = []

        # Add handlers
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name."""
    LoggerConfig.get_instance()  # Ensure logging is configured
    return logging.getLogger(name)
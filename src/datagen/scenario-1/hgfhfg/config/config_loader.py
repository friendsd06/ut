import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional, Union
from yaml.loader import SafeLoader

from exceptions import ValidationError, ConfigurationError
from models import DatabaseConfig, AppConfig
from path_manager import PathManager


class ConfigLoader:
    """Load and manage application configuration."""

    def __init__(self, config_path: Union[str, Path] = "config/config.yaml"):
        """
        Initialize configuration loader.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = PathManager.normalize_path(config_path, is_file=True)
        self._raw_config: Dict[str, Any] = {}
        self.db_config: Optional[DatabaseConfig] = None
        self.app_config: Optional[AppConfig] = None
        self._load_config()

    def _load_yaml(self) -> None:
        """Load YAML configuration file."""
        try:
            PathManager.validate_access(self.config_path)
            with open(self.config_path) as f:
                self._raw_config = yaml.load(f, Loader=SafeLoader)
        except Exception as e:
            raise ConfigurationError(f"Failed to load config file: {str(e)}")

    def _validate_structure(self) -> None:
        """Validate configuration structure."""
        required = {
            'database': {'host', 'port', 'database', 'user', 'password'},
            'app': {'template_directory', 'output_directory', 'log_level'}
        }

        for section, fields in required.items():
            if section not in self._raw_config:
                raise ValidationError(f"Missing section: {section}")

            missing = fields - set(self._raw_config[section].keys())
            if missing:
                raise ValidationError(
                    f"Missing fields in {section}: {', '.join(missing)}"
                )

    def _get_env_value(self, value: str) -> str:
        """Process environment variable references."""
        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
            env_var = value[2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                raise ValidationError(f"Environment variable not set: {env_var}")
            return env_value
        return value

    def _process_env_vars(self) -> None:
        """Process all environment variables in configuration."""
        for section in self._raw_config.values():
            for key, value in section.items():
                section[key] = self._get_env_value(value)

    def _setup_paths(self) -> None:
        """Setup and validate application paths."""
        app_config = self._raw_config['app']

        # Setup base directory
        base_dir = PathManager.normalize_path(
            app_config.get('base_directory', os.getcwd()),
            create=True
        )

        # Setup and validate other directories
        template_dir = PathManager.normalize_path(
            app_config['template_directory'],
            base_dir=base_dir,
            create=True
        )
        PathManager.validate_access(template_dir)

        output_dir = PathManager.normalize_path(
            app_config['output_directory'],
            base_dir=base_dir,
            create=True
        )
        PathManager.validate_access(output_dir, check_write=True)

        # Update config with resolved paths
        app_config.update({
            'base_directory': base_dir,
            'template_directory': template_dir,
            'output_directory': output_dir
        })

    def _create_config_objects(self) -> None:
        """Create typed configuration objects."""
        db_cfg = self._raw_config['database']
        app_cfg = self._raw_config['app']

        self.db_config = DatabaseConfig(
            host=db_cfg['host'],
            port=int(db_cfg['port']),
            database=db_cfg['database'],
            user=db_cfg['user'],
            password=db_cfg['password']
        )

        self.app_config = AppConfig(
            template_directory=app_cfg['template_directory'],
            output_directory=app_cfg['output_directory'],
            log_level=app_cfg['log_level'],
            base_directory=app_cfg['base_directory']
        )

    def _load_config(self) -> None:
        """Load and process configuration."""
        self._load_yaml()
        self._validate_structure()
        self._process_env_vars()
        self._setup_paths()
        self._create_config_objects()

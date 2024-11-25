import os
from pathlib import Path
from typing import Optional, Union
from exceptions import PathError


class PathManager:
    """Handles cross-platform path operations."""

    @classmethod
    def normalize_path(
            cls,
            path: Union[str, Path],
            base_dir: Optional[Path] = None,
            create: bool = False,
            is_file: bool = False
    ) -> Path:

        try:
            path_obj = Path(path)

            if base_dir:
                path_obj = base_dir / path_obj

            path_obj = path_obj.resolve()

            if create:
                cls._create_path(path_obj, is_file)

            return path_obj

        except Exception as e:
            raise PathError(f"Failed to process path '{path}': {str(e)}")

    @staticmethod
    def _create_path(path: Path, is_file: bool) -> None:
        """Create directory or parent directory for files."""
        try:
            target_dir = path.parent if is_file else path
            target_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise PathError(f"Failed to create directory '{path}': {str(e)}")

    @classmethod
    def validate_access(cls, path: Path, check_write: bool = False) -> None:

        if not path.exists():
            raise PathError(f"Path does not exist: {path}")

        if not os.access(path, os.R_OK):
            raise PathError(f"No read access: {path}")

        if check_write and not os.access(path, os.W_OK):
            raise PathError(f"No write access: {path}")
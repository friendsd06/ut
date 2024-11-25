from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    port: int
    database: str
    user: str
    password: str

    def get_database_url(self) -> str:
        """Generate database connection URL."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class AppConfig:
    """Application configuration settings."""
    template_directory: Path
    output_directory: Path
    log_level: str
    base_directory: Path

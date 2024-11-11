class DAGGeneratorError(Exception):
    """Base exception for DAG generator errors."""
    pass


class ConfigurationError(DAGGeneratorError):
    """Raised when configuration is invalid."""
    pass


class TemplateError(DAGGeneratorError):
    """Raised when template rendering fails."""
    pass

class ConfigurationError(Exception):
    """Base exception for configuration-related errors."""
    pass


class PathError(ConfigurationError):
    """Exception raised for path-related errors."""
    pass


class ValidationError(ConfigurationError):
    """Exception raised for configuration validation errors."""
    pass
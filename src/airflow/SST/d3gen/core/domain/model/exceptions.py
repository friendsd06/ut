class DAGGeneratorError(Exception):
    pass

class ConfigurationError(DAGGeneratorError):
    pass

class TemplateError(DAGGeneratorError):
    pass

class ValidationError(DAGGeneratorError):
    pass
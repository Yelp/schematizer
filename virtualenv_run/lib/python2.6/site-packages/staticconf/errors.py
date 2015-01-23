
class ValidationError(Exception):
    """Thrown when a configuration value can not be coerced into the expected
    type by a validator.
    """
    pass


class ConfigurationError(Exception):
    """Thrown when there is an error loading configuration from a file or
    object.
    """
    pass

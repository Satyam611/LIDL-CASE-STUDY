"""
This module defines all the custom exceptions used in the application
"""


class CustomExceptions(Exception):
    """
    Custom class for all custom exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message


class SparkException(CustomExceptions):
    """
    Exception class for all spark related exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message


class ConfigException(CustomExceptions):
    """
    Exception class for all config related exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message


class ArgumentException(CustomExceptions):
    """
    Exception class for all command line argument related exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message


class LoggingException(CustomExceptions):
    """
    Exception class for all logging related exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message


class ETLException(CustomExceptions):
    """
    Exception class for all etl related exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message


class ETLJobException(CustomExceptions):
    """
    Exception class for all etl job related exceptions
    """
    def __init__(self, message: str, *args) -> None:
        super().__init__(message, *args)
        self.message = message

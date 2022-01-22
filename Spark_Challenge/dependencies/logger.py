"""
This module is used for logging purpose. It will create a root logger which will be used throughout the application.
"""
import logging
from dependencies.exception import LoggingException


class Logging:
    def __init__(self, log_level: int = logging.INFO):
        """
        This constructor will create a logger
        :param log_level: Level of logging
        """
        try:
            self.logger = logging.getLogger('root')
            self.logger.setLevel(log_level)
            handler = logging.StreamHandler()
            handler.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(module)s : %(lineno)d : '
                                          '%(message)s', "%Y-%m-%d %H:%M:%S")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        except Exception as le:
            raise LoggingException("Error while initializing logging.", le)

    def get_logger(self) -> logging.Logger:
        """
        This function return Logger object when called
        :return: Logger object
        """
        try:
            return self.logger
        except Exception as le:
            raise LoggingException("Error while getting logger.", le)

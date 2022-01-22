"""
This module is to read and provide the config details back to the calling module
"""
import json
import logging
from dependencies.exception import ConfigException


class Config:
    def __init__(self, config_path: str = ""):
        """
        This constructor will initialize the Config class object and it takes config path as input
        :param config_path: Path of config file provided as a command line argument
        """
        try:
            self.log = logging.getLogger('root')
            self.config_path = config_path
            self.config_content = {}
        except Exception as ce:
            raise ConfigException("Error initializing config class.", ce)

    def read_config(self) -> None:
        """
        This function will read the config file from the provided path as command line argument
        :return: None
        """
        try:
            self.log.info("Reading config file from path : {}".format(self.config_path))
            self.config_content = dict(json.load(open(self.config_path, 'r')))
            self.log.info("Config file read successfully.")

        except Exception as ce:
            raise ConfigException("Error reading config file.", ce)

    def get_config(self) -> dict:
        """
        This function will return the config details.
        :return: Config Details Dictionary
        """
        try:
            self.log.info("Providing config details")
            return self.config_content

        except Exception as ce:
            raise ConfigException("Error providing config details.", ce)

    def validate_config_details(self, config_content=None) -> None:
        """
        This function will validate the config details provided in config file
        :param config_content: Config details dictionary
        :return: None
        """
        try:
            required_details = {'input_path', 'input_file_format', 'write_mode', 'output_path', 'output_file_format'}

            if config_content is None:
                self.log.error("Invalid config details provided.")
                raise ConfigException("Invalid Config Details Provided.")

            if required_details.issubset(config_content.keys()):
                self.log.info("All required details are present in config files.")
            else:
                self.log.error("Please provide all required details in config file. Required details : {}"
                               .format(str(required_details)))
                raise ConfigException("Not all required config details provided in config file.")

            if config_content['application_name'] == '':
                self.log.warning("Application name is not provided in config file. A default value will be used.")
            elif config_content['input_path'] == '':
                self.log.error("Input path value not provided in config file. Property name : 'input_path'")
                raise ConfigException("Input path not provided in config file.")
            elif config_content['input_file_format'] == '':
                self.log.error("Input file format value not provided in config file. Property name "
                               ": 'input_file_format'")
                raise ConfigException("Input file format not provided in config file.")
            elif config_content['write_mode'] == '':
                self.log.error("File Write Mode value not provided in config file. Property name : 'write_mode'")
                raise ConfigException("File write mode not provided in config file.")
            elif config_content['output_path'] == '':
                self.log.error("File Output Path path not provided in config file. Property name : 'output_path'")
                raise ConfigException("Data Output Path not provided in config file.")
            elif config_content['output_file_format'] == '':
                self.log.error("File Output Format not provided in config file. Property name : 'output_file_format'")
                raise ConfigException("File output format not provided in config file.")
            else:
                self.log.info("Config details have been validated successfully.")

        except Exception as ce:
            raise ConfigException("Error validating config details.", ce)

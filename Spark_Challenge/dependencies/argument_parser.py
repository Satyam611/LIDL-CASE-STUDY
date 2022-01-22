"""
This module is for adding and parsing command line arguments
"""

import argparse
import logging
from typing import Any
from dependencies.exception import ArgumentException


class ArgumentParse:
    def __init__(self):
        """
        This constructor initializes the parser class object
        """
        try:
            self.log = logging.getLogger('root')
            self.log.info("Creating argument parser object")
            self.parser = argparse.ArgumentParser()
            self.log.info("Argument parser object created")

        except Exception as ae:
            raise ArgumentException("Error Creating Argument Parser Object", ae)

    def get_parser(self) -> argparse.ArgumentParser:
        """
        This function returns parser object when called.
        :return: ArgumentParser
        """
        try:
            self.log.info("Getting Argument Parser Object")
            return self.parser
        except Exception as ae:
            raise ArgumentException("Error Returning Argument Parser object", ae)

    def add_argument(self, arg_name: str, help_str: str, data_type: Any) -> None:
        """
        This function add a command line argument when called. It takes argument name, helper string and data type of
        the argument
        :param arg_name: Name of command line argument
        :param help_str: Helper string describing the argument
        :param data_type: Data type of argument
        :return: None
        """
        try:
            self.log.info("Adding command line argument : {}".format(arg_name))
            self.parser.add_argument(arg_name, help=help_str, type=data_type)
            self.log.info("Command line argument added")

        except Exception as ae:
            raise ArgumentException("Error while adding command line argument : {}".format(arg_name), ae)

    def get_argument(self) -> argparse.Namespace:
        """
        This function returns all the added arguments to the calling program
        :return: Namespace object
        """
        try:
            self.log.info("Getting all the add command line arguments")
            args = self.parser.parse_args()
            return args

        except Exception as ae:
            raise ArgumentException("Error while getting command line arguments.", ae)

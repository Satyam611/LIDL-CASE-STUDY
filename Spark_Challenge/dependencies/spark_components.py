"""
This module is for creating and handling all the spark session related details and objects.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from dependencies.exception import SparkException


class SparkComponents:
    def __init__(self, application_name: str = "SparkJob"):
        """
        This constructor will create a spark session object when the SparkComponents class object is created
        :param application_name: Spark Application Name provided in config file
        """
        try:
            self.log = logging.getLogger('root')
            self.log.info("Initializing Spark Session")
            self.spark = SparkSession\
                .builder\
                .appName(application_name)\
                .enableHiveSupport()\
                .getOrCreate()
            self.log.info("Spark Session Initialized")

        except Exception as se:
            raise SparkException("Error Creating Spark Session.", se)

    def set_spark_configurations(self):
        pass

    def get_spark_session(self) -> SparkSession:
        """
        This function will return spark session object when called
        :return: Spark Session Object
        """
        try:
            self.log.info("Providing Spark Session")
            return self.spark
        except Exception as se:
            raise SparkException("Error getting spark session.", se)

    def stop_spark_session(self) -> None:
        """
        This function will stop the spark session instance when called
        :return: None
        """
        try:
            self.log.info("Stopping spark session")
            return self.spark.stop()
        except Exception as se:
            raise SparkException("Error Stopping Spark Session", se)

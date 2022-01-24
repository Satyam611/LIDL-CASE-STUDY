"""
This is the main module which will use all the other modules to create and execute an ETL job.
"""
import logging
from typing import Tuple

from pyspark.sql.window import Window

from dependencies.logger import Logging
from dependencies.config import Config
from dependencies.spark_components import SparkComponents
from dependencies.argument_parser import ArgumentParse
from dependencies.exception import ETLJobException, ETLException, ConfigException, SparkException, ArgumentException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower, avg, window, count, row_number, dense_rank
import pyspark.sql.functions as f


class ETL:
    def __init__(self, config: dict, spark: SparkSession):
        try:
            self.log = logging.getLogger('root')
            self.log.info("Initializing ETL Object")
            self.job_config = config
            self.spark = spark
            spark.conf.set('spark.sql.session.timeZone', 'UTC')
        except Exception as ee:
            raise ETLException("Error initializing ETL Object.", ee)

    def extract(self) -> DataFrame:
        """
        This function extracts data from the given input path in config file and returns a spark dataframe.
        :return: Spark Dataframe
        """
        try:
            self.log.info("Data Extraction Started")
            input_df = self.spark\
                .read\
                .options(**self.job_config['read_options'])\
                .format(self.job_config['input_file_format'])\
                .load(self.job_config['input_path'])
            self.log.info("Data Extraction Completed")
            return input_df

        except Exception as ee:
            raise ETLException("Error Extracting data.", ee)

    def transform(self, input_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        This function takes a spark dataframe as input, transform it as per business logic and returns a spark
        dataframe
        :param input_df: Input Dataframe
        :return: Output Dataframe
        """
        try:
            self.log.info("Data Transformation Started")
            granularity_in_minutes = int(self.job_config['granularity_time'])
            output_df = input_df\
                .withColumn("action", f.initcap(col("action")))\
                .drop(input_df.action) \
                .filter(col("action").isin(['Open', 'Close'])) \
                .groupBy(window("time", str(granularity_in_minutes) + " minutes"))\
                .pivot("action")\
                .agg(count("action"))\

            output_df = output_df\
                .withColumn("Close", f.when(col("Close").isNull(), 0).otherwise(col("Close")))\
                .drop(output_df.Close) \
                .withColumn("Open", f.when(col("Open").isNull(), 0).otherwise(col("Open"))) \
                .drop(output_df.Open)\
                .withColumn("total_actions_every_ten_min", col("Close") + col("Open")) \
                .withColumn("close_per_min", col("Close") / granularity_in_minutes)\
                .withColumn("open_per_min", col("Open") / granularity_in_minutes)\
                .withColumn("total_actions_per_min", col("total_actions_every_ten_min") / granularity_in_minutes)\
                .withColumn("window_start_time", output_df.window.start)\
                .withColumn("window_end_time", output_df.window.end)\
                .orderBy("window_start_time").drop("window")\

            window_spec = Window.orderBy(col("Open").desc())
            max_actions_df = output_df\
                .withColumn("rank", dense_rank().over(window_spec))\
                .filter("rank = 1")\
                .drop("rank")
            self.log.info("Data Transformation Completed")
            return output_df, max_actions_df

        except Exception as ee:
            raise ETLException("Error transforming data.", ee)

    def load(self, output_df: DataFrame, max_actions_df: DataFrame) -> None:
        """
        This function takes a spark dataframe as input and writes it to path provided input config file
        :param max_actions_df: Spark dataframe with records with maximum actions in 10 mins
        :param output_df: Spark Dataframe
        :return: None
        """
        try:
            self.log.info("Data Loading Started")
            self.log.info("Writing Output Dataframe")
            output_df.coalesce(1)\
                .write.options(**self.job_config['write_options']) \
                .format(self.job_config['output_file_format']) \
                .mode(self.job_config['write_mode']) \
                .save(self.job_config['output_path'] + "complete/")
            self.log.info("Output Dataframe Write Completed")
            self.log.info("Writing Max Actions Dataframe")
            max_actions_df.coalesce(1) \
                .write.options(**self.job_config['write_options']) \
                .format(self.job_config['output_file_format']) \
                .mode(self.job_config['write_mode']) \
                .save(self.job_config['output_path'] + "max_actions/")
            self.log.info("Max Actions Dataframe Write Completed")
            self.log.info("Data Loading Completed")

        except Exception as ee:
            raise ETLException("Error loading data to output path.", ee)


class EtlJob:
    def __init__(self, job_config: dict):
        """
        This constructor initializes ETL Job details
        :param job_config: Config Details Dictionary
        """
        try:
            self.log = logging.getLogger('root')
            self.log.info("Initializing ETL Job Object")
            self.job_config = job_config
            self.spark_comp = SparkComponents(self.job_config['application_name'])
            self.log.info("ETL Job Object Initialized")
        except Exception as eje:
            raise ETLJobException("Error in ETL Job.", eje)

    def run_job(self) -> None:
        """
        This function run the extract, transform and load tasks sequentially
        :return: None
        """
        try:
            self.log.info("Running ETL Job")
            etl = ETL(self.job_config, self.spark_comp.get_spark_session())
            self.log.info("Starting Input Data Extraction")
            input_df = etl.extract()
            self.log.info("Input Data Extraction Completed")
            self.log.info("Starting Data Transformation")
            output_df, max_actions_df = etl.transform(input_df)
            self.log.info("Data Transformation completed")
            self.log.info("Starting Output Data Loading")
            etl.load(output_df, max_actions_df)
            self.log.info("Output Data Loading Completed")
            self.log.info("ETL Completed")
            self.log.info("Stopping Spark Session Object")
            self.spark_comp.stop_spark_session()
            self.log.info("Spark Session Object stopped")

        except Exception as ee:
            raise ETLJobException("Error Running ETL Job.", ee)


def main() -> None:
    """
    This is main function and it will setup the job and call the running components
    :return: None
    """
    log.info("Validating Command line arguments.")

    # setting job status to false
    job_status = False
    try:
        log.info("Reading command line arguments")
        log.info("Starting parsing of command line arguments")
        # creating Argument Parser object
        parser = ArgumentParse()
        # adding command line argument
        parser.add_argument('config_file_path', "Path for config file", str)
        # getting all the command line arguments
        args = parser.get_argument()

        log.info("Config file path provided : {}".format(args.config_file_path))
        config = Config(args.config_file_path)
        # reading config file from provided path
        config.read_config()
        # getting config details
        config_details = config.get_config()
        # validating config details to check if all the required details are present in config file
        config.validate_config_details(config_details)

        # starting ETL job
        log.info("Starting ETL Job")
        job = EtlJob(config_details)
        log.info("Running Job")
        # executing job
        job.run_job()
        # setting job status to true
        job_status = True

    except SparkException as se:
        log.error(se)
    except ConfigException as ce:
        log.error(ce)
    except ArgumentException as ae:
        log.error(ae)
    except ETLException as ee:
        log.error(ee)
    except ETLJobException as eje:
        log.error(eje)

    finally:
        # Evaluating job status
        if job_status:
            log.info("Job Completed Successfully.")
            exit(0)
        else:
            log.error("Job Failed.")
            exit(1)


# Entry point for spark application
if __name__ == "__main__":
    # creating logging object
    log = Logging(logging.INFO).get_logger()
    log.info("Calling main function")
    main()

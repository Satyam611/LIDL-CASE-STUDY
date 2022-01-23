"""
test_cases.py
This module is for unit testing of the spark application
"""
import logging
import sys
import unittest

from pyspark.sql.functions import col
import pyspark.sql.functions as f
sys.path.append(r'C:\Users\satya\Documents\GitHub\LIDL-CASE-STUDY\Spark_Challenge')
from dependencies.logger import Logging
from dependencies.spark_components import SparkComponents
from dependencies.config import Config
from jobs.job import ETL


class SparkTestCases(unittest.TestCase):
    """
    Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """
        Start Spark, define config and path to test data
        """
        self.log = Logging(logging.INFO).get_logger()
        config_obj = Config('./test_data/config/config.json')
        config_obj.read_config()
        self.config = config_obj.get_config()
        config_obj.validate_config_details(self.config)
        self.spark = SparkComponents(self.config['application_name']).get_spark_session()
        self.etl = ETL(self.config, self.spark)
        self.input_df = self.etl.extract()
        self.output_df, self.max_actions_df = self.etl.transform(self.input_df)

        self.complete_output_test_df = self.spark\
            .read\
            .options(**self.config['read_options'])\
            .format('csv')\
            .load(self.config['output_path'] + "complete") \
            .select(col("Close").cast("long"),
                    col("Open").cast("long"),
                    col("total_actions_every_ten_min").cast("long"),
                    col("close_per_min"),
                    col("open_per_min"),
                    col("total_actions_per_min"),
                    col("window_start_time"),
                    col("window_end_time"))

        self.max_actions_test_df = self.spark \
            .read \
            .options(**self.config['read_options'])\
            .format('csv') \
            .load(self.config['output_path'] + "max_actions")\
            .select(col("Close").cast("long"),
                    col("Open").cast("long"),
                    col("total_actions_every_ten_min").cast("long"),
                    col("close_per_min"),
                    col("open_per_min"),
                    col("total_actions_per_min"),
                    col("window_start_time"),
                    col("window_end_time"))

    def tearDown(self):
        """
        Stop Spark
        """
        self.spark.stop()

    def test_number_of_columns_for_output(self):
        complete_output_test_df_col_count = len(self.complete_output_test_df.columns)
        output_df_col_count = len(self.output_df.columns)
        self.assertEqual(output_df_col_count, complete_output_test_df_col_count)

    def test_column_names_for_output(self):
        complete_output_test_df_col = self.complete_output_test_df.columns
        output_df_col = self.output_df.columns
        self.assertEqual(output_df_col, complete_output_test_df_col)

    def test_number_of_rows_for_output(self):
        complete_output_test_df_count = self.complete_output_test_df.count()
        output_df_count = self.output_df.count()
        self.assertEqual(output_df_count, complete_output_test_df_count)

    def test_data_type_for_output(self):
        complete_output_test_df_dtypes = self.complete_output_test_df.dtypes
        output_df_dtypes = self.output_df.dtypes
        self.assertEqual(output_df_dtypes, complete_output_test_df_dtypes)

    def test_data_for_output(self):
        self.assertEqual(self.output_df.subtract(self.complete_output_test_df).count(), 0)

    def test_number_of_columns_for_max_actions(self):
        max_actions_test_df_col_count = len(self.max_actions_test_df.columns)
        max_actions_df_col_count = len(self.max_actions_df.columns)
        self.assertEqual(max_actions_df_col_count, max_actions_test_df_col_count)

    def test_column_names_for_max_actions(self):
        max_actions_test_df_col = self.max_actions_test_df.columns
        max_actions_df_col = self.max_actions_df.columns
        self.assertEqual(max_actions_df_col, max_actions_test_df_col)

    def test_number_of_rows_for_max_actions(self):
        max_actions_test_df_count = self.max_actions_test_df.count()
        max_actions_df_count = self.max_actions_df.count()
        self.assertEqual(max_actions_df_count, max_actions_test_df_count)

    def test_data_type_for_max_actions(self):
        max_actions_test_df_dtypes = self.max_actions_test_df.dtypes
        max_actions_df_dtypes = self.max_actions_df.dtypes
        self.assertEqual(max_actions_df_dtypes, max_actions_test_df_dtypes)

    def test_data_for_max_actions(self):
        self.assertEqual(self.max_actions_df.subtract(self.max_actions_test_df).count(), 0)


if __name__ == '__main__':
    unittest.main()

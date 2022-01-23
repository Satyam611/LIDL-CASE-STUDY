# LIDL-CASE-STUDY(Spark Challenge)

## Answers to questions asked as part of case study
### Can you do a proposal about how to test this job with unit test?

For unit testing we have written a test framework which will do the following testings:
1. Number of columns
2. Column Names
3. Number of rows
4. Data Type of columns
5. Data of both files

Above mentioned test cases will be applied on both the complete output and 
max open actions output dataset. For this we have created a 'tests' folder which
has below mentioned folders:
- tests
  - test_data
    - config
      - config.json
    - input_data
      - input.csv
    - output_data
      - complete
        - output.csv
      - max_actions
        - output.csv
  - test_cases.py
    
### how to test a full pipeline with a integration test?

To test the full pipeline, we should put some mock data in the storage and provide
the details in config file accordingly. After that run the etl job with spark-submit command 
or using any orchestrator as it will run in production.

### how to release this job on production with data quality check?

For data validation, we can have another small script which will run prior to our etl job
and validate the data. This script can be in pyspark or python using pandas.
Data Validations to do:
1. Check if any event is missing timestamp
2. Check if there is any action other than 'Open' and 'Close'
3. Check if the time format is correct
4. Check if the actions are in same case or not, if not transform them to
single case

## Approach

The approach in this application was to develop all the components separately so as to increase modularity and reusability, and then bring them together to 
create a complete application. Also, since we are using a config file to configure the application, so it can also be used as sa metadata driven framework.

### Below components(dependencies) are created as part of this application development:
1. configs.py : This module handles all the tasks related to configuration i.e., config file reading and provide 
   the config details
2. argument_parser.py : This module takes care of argument parsing i.e., command line arguments. It parses the arguments 
   provided in command line and then returns them to calling module
3. exception.py : This module is created to handle the custom exceptions that we are using in our application.
4. spark_components.py : This module is created to handle spark session object. This module creates, provides and 
   stop the spark session object.
5. logger.py : This module takes care of logging part of the application. It creates a 'root' logger which can be 
   used by all other components for logging purpose.

### Main Application:
job.py : This is the main application that uses all the other components and run to perform the processing. It has 2 
classes ETL and ETLJob. ETL class has three functions extract, transform and load. Extract is used to read the data 
from input path. Transform is for implementing the business logic. Load is for writing the output data to the path 
provided in the config file.

### Config file:
config.json : We have created one json file which provides the details to job to run accordingly.
	
### Test Data:
We are using input file provided as test data.

## Instructions to run the application

To run this spark application we have to run the below mentioned command(provided for both Windows and Linux). In this 
command 'dependencies.zip' has all the modules of this application like(config.py, logging.py, argument_parser.py, 
spark_components.py, exception.py). These modules will be used during the execution of the application.

At the end we have provided './configs/config.json'. This is the path of config file which will be used by the 
application.

Before running this application, we have to be make sure that we are at the 'Spark Challenge' folder level. That is 
we should run this command from outside of 'jobs' folder. If not then this command needs to be edited as per the new 
paths.

### For Windows

%SPARK_HOME%/bin/spark-submit --py-files dependencies.zip ./jobs/job.py ./configs/config.json

### For Linux

$SPARK_HOME/bin/spark-submit --py-files dependencies.zip ./jobs/job.py ./configs/config.json
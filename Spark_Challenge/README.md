# LIDL-CASE-STUDY(Spark Challenge)

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
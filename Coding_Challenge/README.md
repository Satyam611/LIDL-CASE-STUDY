# LIDL-CASE-STUDY(Coding Challenge)

### 1. Describe a solution for the previous case when the data does not fit in memory

We can use distributed file system to process data that does not fit in memory. In this case our file will be 
divided into multiple parts. And now we can apply our logic to get the pairs with a particular sum. Best tool 
for this can be Apache Spark.
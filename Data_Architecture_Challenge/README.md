# LIDL-CASE-STUDY(Data Architecture Challenge)

### 1. What tracking events would you propose? What data model for event analysis? What technologies? 

Tracking Events:
1. User Entry
2. User Exit
3. Car Parking Full
4. Number of Free Car Parkings
5. Payment Done
6. Payment Pending

Data Model:
We can create below tables in our warehouse to do further analysis
1. User
2. Payment
3. Parking_Lot
4. Events
5. Vehicle

Technologies:
1. IOT Service
2. Kafka
3. Spark
4. S3/Redshift

### 2. How would you design the Backend system? What data model for the Operational system? What technologies?

Backend can have below services:
1. User Entry/Exit service
2. Parking service
3. Spot Allocation/Deallocation service
4. Pricing service

Databases:
1. Parking Spot Database
2. Payment Database
3. Event History Database
4. User Database

Data Model:
1. Entity-Relationship Data Model

### 3. Explain how to combine the operational architecture with the analytical one?

To connect operational architecture with analytical we can either create APIs or use an ingestion tool which can fetch 
the data from OLTP Database and put it in datalake or in the data warehouse. After data has landed in our analytical 
architecture we can do further processing and transformations to get business insights

### 4. Could you propose a process to manage the development lifecycle? And the test and deployment automation?
Development Lifecycle

We can use Agile Methodology for our software development as it will help us in delivering functionalities in an 
efficient and fast way.

Testing and Deployment

We can CI/CD for testing and deployment. Whenever a developer commits changes in repository a jenkins pipeline will be
triggered and the changes will be build, tested and deployed in the dev, test and prod environment.

For infrastructure creation also we can use services like cloudformation or terraform to automate the deployment.
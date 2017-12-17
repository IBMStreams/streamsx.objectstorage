# Object Storage Toolkit 

# Streams Designer Integration Tests

 

Object Storage Toolkit Version: 1.0.0.2



## Test 1

 

1.      **Flow name:** [1KSmallPartitions]()
2.      **Test target:** check ObjectStorageSink operator behavior with  big number (1K) of simultaneously opened partitions 
3.      **Flow description:**1000 tuples + 20 tuples/sec
4.      ** Flow structure:**

​       ![Test1-Img1](C:\AAA\Streams\pipelines\objectstorage\IntegrationTest\docs\images\Test1-Img1.png)                      

5. **** Object Storage Sink Configuration **

 ![Test1-Img2](C:\AAA\Streams\pipelines\objectstorage\IntegrationTest\docs\images\Test1-Img2.png)

6.      **Test Results:** all objects have been created as expected and with a required content. Pace of output objects creation is relatively slow.

** **

 

## Tests 2-4

 

1.      **Flow name:** CSVTestNoVars_[2-4]

2.      **Test target:** 

Object Storage Operator Configuration:

\1. Storage format  =csv

\2. Test 2-> Rolling policy = time (10 mins)

​     Test 3 -> Rolling policy = size (100K)

​     Test 4 -> Rolling policy = count(3000)

\3. No variables in the output object name

Expected output: Single object ofCSV format refreshed every 5 minutes

**3.      ****Data Source:** DataHistorian Sample Data****

**4.      ****Flow structure:**

 

 

**5.      ****Object Storage SinkConfiguration**

 

**6.      ****Test Results: **asexpected. (Remark: all three flows are executed in parallel with the output tothe different objects of the same bucket).****

 

## Tests 5-7

 

1.      **Flow name:** MultipleParquetPartitions_[5-7]

2.      **Test target:** 

Object Storage Operator Configuration:

\1. Storage format  = parquet

\2. Test 5-> Rolling policy = time (5 mins) + Uncompressed

​    Test 6 -> Rolling policy = size (100K)+ Gzip

​    Test 7 -> Rolling policy = count (500)+Snappy

\3. %TIME variable 

\4. Four levels of partitioning

**3.      ****Data Source: **ClickStream Sample Data****

**4.      ****Flow structure:**

 

 

**5.      ****Object Storage SinkConfiguration**

 

6.      **Test Output:**

**  **

The test checks:

-         Automatic partitioninsertion before object name

-         Null partitions creation

 

 

## Tests 8-10

 

1.      **Flow name:** MultipleParquetPartitions

2.      **Test target:** 

Object Storage Operator Configuration:

\1. Storage format  = parquet

\2. Test 5-> Rolling policy = time (5 mins) + Uncompressed

​    Test 6 -> Rolling policy = size (100K)+ Gzip

​    Test 7 -> Rolling policy = count (5000)+ Snappy

\3. %OBJECTNUM variable

\4. Three levels of partitioning

\5. Object path consists of *prefix1/prefix2/objectName_%OBJECTNUM.parquet*

**3.      ****Data Source: **ClickStream Sample Data****

**4.      ****Flow structure:**

 

 

**5.      ****Object Storage SinkConfiguration**

 

6.      **Test Output:**

After 10 minutes of running wehave two objects per partition (total partitions number is 34):

** **

 

​                

## Tests 11

 

1.      **Flow name:** MultipleSinkInstances

2.      **Test target:** 

The test contains three sink operators:

-         Parquet + partitioning by *customerId*+ snappy compression + close by object size

-         CSV + time rolling policy(close every 10 mins)

-         Parquet + no partition +gzip compression  + close by event count(1000 events)

**3.      ****Data Source:  **Geofence Sample Data****

**4.      ****Flow structure:**

 

 

 

5.      **Test Runtime**: 30minutes

6.      **Test Output:**

After 10 minutes of running wehave two objects per partition (total partitions number is 34):

** **

 

 
Performance Tests
=================

Object Storage Toolkit Version: 1.0.0.3

EmptyJavaOpTest
----------------

1.  **SPL/Composite name:** [EmptyJavaOpTest.spl](https://github.com/IBMStreams/streamsx.objectstorage/blob/performance/test/performance/com.ibm.streams.objectstorage.generic.test/com.ibm.streamsx.objectstorage.generic.perftest/EmptyJavaOpTest.spl)

2.  **Scenario:** Beacon + Empty Java operator

3.  **Fusion Type:** Fused

4.  **Input:** Data Historian structure. All input tuples contains the *same* data.
 
Object Storage Sink CSV - Test1
-------------------------------

1.  **SPL/Composite name:** [ObjectStorageSinkCSVTest1.spl](https://github.com/IBMStreams/streamsx.objectstorage/blob/performance/test/performance/com.ibm.streams.objectstorage.generic.test/com.ibm.streamsx.objectstorage.generic.perftest/ObjectStorageSinkCSVTest1.spl)

2.  **Scenario:** Beacon + Format + OSSink operator. The scenario used for CVS formatted objects creation.

3.  **Fusion Type:** Fused

4.  **Input:** Data Historian structure. All input tuples contains the *same* data.

5.  **Rolling policy:** Tuple count = 1M.
 
Object Storage Sink Parquet - Test 1
------------------------------------

1.  **SPL/Composite name:** [ObjectStorageSinkParquetTest1.spl](https://github.com/IBMStreams/streamsx.objectstorage/blob/performance/test/performance/com.ibm.streams.objectstorage.generic.test/com.ibm.streamsx.objectstorage.generic.perftest/ObjectStorageSinkParquetTest1.spl)

2.  **Scenario:** Beacon + OSSink operator . The scenario used for SNAPPY compressed parquet objects creation.

3.  **Fusion Type:** Fused

4.  **Input:** Data Historian structure. All input tuples contains the *same* data.

5.  **Rolling policy:** Tuple count = 1M.
 

 

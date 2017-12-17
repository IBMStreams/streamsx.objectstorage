Object Storage Toolkit Unitests
===============================

Background
----------

The unitests have developed using streamsx.topology testing capabilities and
fully integrated with a standard Junit 4 test framework.

Each unitest can be executed independently and contains the following steps:

-   Test application compilation

-   Test execution

-   Test results validation

Obviously, each unitest contains toolkit operators invocation with a different
parametrization.

The unitests running locally (without object storage connection) and generates
its output in the local filesystem. The approach became possible since the
toolkit operators’ logic is implemented on top of hadoop.fs API. For production
version, stocator or Hadoop-aws hadoop.fs implementation is used. For unitest
purposes the remote clients above were replaced with the local hadoop.fs
implementation provided as a part of hadoop.fs distribution. Thus, as operator
logic is agnostic to the specific hadoop.fs implementation replacement of the
real object storage client with the local hadoop.fs required no changes in the
operator logic and provides convenient and disconnected approach for operator
logic unitesting.

Test Details
------------

The following table summarizes ObjectStorageSink operator unitests. The unitests
can be executed either from Streams Studio or using toolkit’s build.xml.

| **Parquet No Partitioning**                  |                                                                                                                                                                                                                             |
|----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Test Name**                                | **Test Description**                                                                                                                                                                                                        |
| TestCloseBySizeParquetSnappy                 | %OBJECTNUM + Parquet with Snappy Compression + Close Objects By Data Size                                                                                                                                                   |
| TestCloseBySizeParquetGzip                   | %OBJECTNUM + Parquet with Gzip Compression + Close Objects By Data Size                                                                                                                                                     |
| TestCloseBySizeParquetUncompressed           | %OBJECTNUM + Parquet with No Compression + Close Objects By Data Size                                                                                                                                                       |
| TestCloseByTimeParquetSnappy                 | %OBJECTNUM + Parquet with Snappy + Close Objects By Time                                                                                                                                                                    |
| TestCloseByTimeParquetGzip                   | %OBJECTNUM + Parquet with Gzip + Close Objects By Time                                                                                                                                                                      |
| TestCloseByTimeParquetUncompressed           | %OBJECTNUM + Parquet with No Compression + Close Objects By Time                                                                                                                                                            |
| TestCloseByTupleCountParquetSnappy           | %OBJECTNUM + Parquet with Snappy Compression + Close Objects By Tuple Count                                                                                                                                                 |
| TestCloseByTupleCountParquetGzip             | %OBJECTNUM + Parquet with Gzip Compression + Close Objects By Tuple Count                                                                                                                                                   |
| TestCloseByTupleCountParquetUncompressed     | %OBJECTNUM + Parquet with No Compression + Close Objects By Tuple Count                                                                                                                                                     |
| TestParallelSink                             | %OBJECTNUM + Parquet with Gzip + Close Objects By Time + Parallelity Level=3                                                                                                                                                |
| TestSinkMetrics                              | %OBJECTNUM + Parquet with Gzip + Close Objects By Time + Get Sink Operator Metrics                                                                                                                                          |
| TestStaticObjectName                         | Parquet with No Compression + Close Objects By Tuple Count                                                                                                                                                                  |
|                                              |                                                                                                                                                                                                                             |
| **Parquet With Partitioning**                |                                                                                                                                                                                                                             |
| **Test Name**                                | **Test Description**                                                                                                                                                                                                        |
| TestCloseBySizeParquetAutoPartitioning       | %OBJECTNUM + Add partition automatically after prefix and before object name + with prefix of depth 2 + Close by Size + Partition of Depth 1                                                                                |
| TestCloseBySizeParquetPartitionVar           | %OBJECTNUM + %PARTITIONS: Add partition after prefix and before suffix according to %PARTITIONS variable location + with prefix of depth 1 + suffix of depth 1 + Close by Size                                              |
| TestCloseByTimeParquetAutoPartitioning       | %OBJECTNUM + Add partition automatically after prefix and before object name + with prefix of depth 2 + Close by Time + Partition of Depth 1                                                                                |
| TestCloseByTimeParquetPartitionVar           | %OBJECTNUM + %PARTITIONS: Add partition after prefix and before suffix according to %PARTITIONS variable location + with prefix of depth 1 + suffix of depth 1 + Close by Time                                              |
| TestCloseByTupleCountParquetAutoPartitioning | %OBJECTNUM + Add partition automatically after prefix and before object name + with prefix of depth 2 + Close by Tuple Count + Partition of Depth 1                                                                         |
| TestCloseByTupleCountParquetPartitionVar     | %OBJECTNUM + %PARTITIONS: Add partition after prefix and before suffix according to %PARTITIONS variable location + with prefix of depth 1 + suffix of depth 1 + Close by Tuple Count                                       |
| TestEmptyPartition                           | %OBJECTNUM + Add partition automatically after prefix and before object name + with prefix of depth 2 + Close by Size + Partition of Depth 1 + Automatically creates empty partition for missing partition attribute values |
|                                              |                                                                                                                                                                                                                             |
|                                              |                                                                                                                                                                                                                             |
| **CSV No Partitiong**                        |                                                                                                                                                                                                                             |
| **Test Name**                                | **Test Description**                                                                                                                                                                                                        |
| TestCloseBySizeComplexInSchema               | %OBJECTNUM + Tuple with multiple attributes + Close Object by Data Size                                                                                                                                                     |
| TestCloseBySizeSimpleInSchema                | %OBJECTNUM + Tuple with single string attribute + Close Object by Data Size                                                                                                                                                 |
| TestCloseByTimeComplexInSchema               | %OBJECTNUM + Tuple with multiple attributes + Header + Close Object by Data Size                                                                                                                                            |
| TestCloseByTimeSimpleInSchema                | %OBJECTNUM + Tuple with single string attribute + Header + Close Object by Data Size                                                                                                                                        |
| TestCloseByTupleCountComplexInSchema         | %OBJECTNUM + Tuple with multiple attributes + Close Object by Tuple Count                                                                                                                                                   |
| TestCloseByTupleCountSimpleInSchema          | %OBJECTNUM + Tuple with single string attribute + Close Object by Tuple Count                                                                                                                                               |

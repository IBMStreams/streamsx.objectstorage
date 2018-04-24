---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}


# Prerequisite

This toolkit uses Apache Ant 1.8 (or later) to build.

Internally Apache Maven 3.2 (or later) and Make are used.

Download and setup directions for Apache Maven can be found here: http://maven.apache.org/download.cgi#Installation

Set environment variable M2_HOME to the path of maven home directory.

    export M2_HOME=/opt/apache-maven-3.5.0
    export PATH=$PATH:$M2_HOME/bin

# Build the toolkit

Run the following command in the `streamsx.objectstorage` directory:

    ant all

# Build the samples

Run the following command in the `streamsx.objectstorage` directory to build all projects in the `samples` directory:

    ant build-all-samples


# Toolkit Architecture Overview

Following diagram represents the toolkit high-level architecture:
![Import](/streamsx.objectstorage/doc/images/OSToolkitArchitecture.png)
As might be seen, the toolkit implemented on top of `org.apache.hadoop.fs.FileSystem`.
This allowed to inherit some of the [HDSF Toolkit](https://github.com/IBMStreams/streamsx.hdfs) logic.
Yet, as in many aspects `Object Storage` behaves differently from `HDFS`, the current
toolkit version contains significant number of adoptions and optimizations which are
unique for COS. 

Also, the diagram represents two different clients which utilized by the toolkit:
* [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

`hadoop-aws` is an open-source COS connector implementing `org.apache.hadoop.fs.FileSystem`.
`hadoop-aws` has a unique and very powerful [S3A Fast Upload](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Stabilizing:_S3A_Fast_Upload)
feature heavily utilized by the `ObjectStorageSink` operator. The feature allows:
    - uploading of large files as blocks with the size set by `fs.s3a.multipart.size`.
    - buffering of object multipart blocks to the local disk
    - upload the mutlipart blocks in parallel in background thread
  
* [stocator](https://github.com/CODAIT/stocator) 

`stocator` has been developed by IBM and is optimized for IBM COS.

Note, that parquet storage format is implemented with [parquet-mr](https://github.com/apache/parquet-mr).
To be more specific, the [parquet-hadoop](https://github.com/apache/parquet-mr/tree/master/parquet-hadoop) module is used.

## Toolkit Class Diagram
The following class diagram represents main toolkit classes and packages:
![Import](/streamsx.objectstorage/doc/images/OSToolkitHighLevelClassDiagram.png)
As might be seen from the diagram above there are three level of classes in the 
toolkit implementation:
 - `AbstractObjectStorageOperator` contains the logic that is common for all 
   toolkit operators, such as COS connection establishment according to the 
   specific protocol (client).
 - `BaseObjectStorageScan\Source\Sink` contains the  implementation which is common
   for the generic and S3-specific Scan\Source\Sink operators.   
 - `S3ObjectStorageXXX\ObjectStorageXXX` contains parameters specific for S3 and 
   generic operators respectively. Note, that this layer almost doesn't contain business
   logic.


## Sink Operators Implementation Details
![Import](/streamsx.objectstorage/doc/images/OSSinkOperatorArchitecture.png)

# Toolkit Java-based Tests

Current java-based test suite covers `ObjectStorageSink` operator only.
Yet, its important to mention, that the test suite is easily extendable
with additional tests for the `ObjectStorageSink` and other operators.
For more details about the tests and functionality they cover see the 
![Object Storage Toolkit Java-based Tests](/streamsx.objectstorage/tree/master/test/java/com.ibm.streamsx.objectstorage.test)
 
## ObjectStorageSink Tests Class Diagrams

`ObjectStorageSink` tests can be divided to the groups by the storage format:

* tests for the `raw` storage format
The following class diagram represents classes involved in the `raw` storage format testing.
![Import](/streamsx.objectstorage/doc/images/OSToolkitJavaRawTestClassDiagram.png)
`BaseObjectStorageTestSink` is the base class for all Sink operator tests. 
Each specific test implements/overwrites method specific for it.

* tests for the `parquet` storage format
The following class diagram represents classes involved in the `parquet` storage format testing.
![Import](/streamsx.objectstorage/doc/images/OSToolkitJavaParquetTestClassDiagram.png)
`BaseObjectStorageTestSink` is the base class for all Sink operator tests. 
Each specific test implements/overwrites method specific for it.
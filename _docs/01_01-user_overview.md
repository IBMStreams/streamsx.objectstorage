---
title: "Toolkit Usage Overview"
permalink: /docs/user/overview/
excerpt: "How to use this toolkit."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

The open source Object Storage toolkit is used to create and access objects in Object Storage from Streams.  It is been updated with some new features and is also included in Streams 4.3.
The toolkit supports Object Storage services with S3 API like the [IBM Cloud Object Storage](https://cloud.ibm.com/docs/cloud-object-storage) service and provides the following features:

* Create/Delete bucket
* List objects
* Put/get object
* Delete object

## Features

* Write objects in [Apache Parquet](https://parquet.apache.org/) format.
* You can control the rolling policy when writing objects. The rolling policy allows you to determine when to close the current file being written and open a new file. The  operator supports the following rolling policy types:
  * Size-based (parameter bytesPerObject)
  * Time-based (parameter timePerObject)
  * Tuple count-based (parameter tuplesPerObject)
  * Close object when receiving punctuation marker (default)
* At least once processing (consistent region support) for ObjectStorageScan and ObjectStorageSource operator
* Guaranteed processing with exactly-once semantics (consistent region support) with the ObjectStorageSink operator

### Reading objects

* Use the ObjectStorageScan operator similar to the DirectoryScan operator to scan for objects and retrieve the object names
* Use the ObjectStorageSource operator to read objects line by line or in binary format

### Writing objects

Create objects with the ObjectStorageSink operator in line format, blob format or parquet format. It includes Parquet formatted data with Partitioning that is compatible with the [IBM Cloud SQL Query service](https://www.ibm.com/cloud/sql-query) allowing Streams data written to COS to be queried using SQL.


## Client selection

Different clients are utilized by the toolkit operators. You may easily select one of the two S3 clients by specifying appropriate protocol in the objectStorageURI parameter:

* s3a (hadoop-aws)
* cos (stocator)

The URI should be in `cos://<bucket>/` or `s3a://<bucket>/` format. Replace <bucket> with the name of the bucket you [created](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-getting-started-cloud-object-storage). For example, `cos://streams-sample-001/` with bucket name **streams-sample-001**.

For ObjectStorageScan and ObjectStorageSource operator there is no big difference regarding the S3 client and you can select any of both clients.

The ObjectStorageSink works differently depending on the client selection.


### Recommendation for client selection when writing objects

* Your application creates large objects (raw or parquet format) one after another, then select **s3a** protocol, because large objects are uploaded in multiple parts in parallel.
* Your application creates many objects within a narrow time frame, then select **cos** protocol, because multiple threads are uploading the entire object per thread in parallel.
* Your application creates large objects and ObjectStorageSink is part of a consistent region, then select **s3a** protocol and benefit from multi-part upload reducing the object close time at drain state of the region.
* Select the **s3a** protocol to prevent buffering on disk prior upload. When **cos** protocol is selected then data is written to disk before uploading the objects to object storage. The **s3a** protocol uses memory buffer per default.


### SPLDOC

The ObjectStorage toolkit contains three generic operators, the *ObjectStorageScan*, *ObjectStorageSource* and the *ObjectStorageSink*
and three S3-specific operators, the *S3ObjectStorageScan*, *S3ObjectStorageSouce* and the *S3ObjectStorageSink*.
In addition, the toolkit contains set of native functions for the bucket and object management in COS (Cloud Object Storage).

Read more about how to use the operators and functions in the [SPL documentaion](/streamsx.objectstorage/doc/spldoc/html/index.html).

Toolkit version 1.x: [SPL documentaion](/streamsx.objectstorage/doc/spldoc1.x/html/index.html).

### Samples

#### Samples with IAM Authentication schema

* [FunctionsSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/iam/FunctionsSampleIAM)
* [PartitionedParquetSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/iam/PartitionedParquetSampleIAM)
* [SinkScanSourceSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/iam/SinkScanSourceSampleIAM)
* [TimeRollingPolicySample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/iam/TimeRollingPolicySampleIAM)

#### Samples with Basic Authentication schema

* [FunctionsSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/basic/FunctionsSample)
* [PartitionedParquetSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/basic/PartitionedParquetSample)
* [SinkScanSourceSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/basic/SinkScanSourceSample)
* [TimeRollingPolicySample](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/samples/basic/TimeRollingPolicySample)


### Demos

* [FormatDemo-Avro-Json-Parquet](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/demo/com.ibm.streamsx.objectstorage.formats.demo)
* [ObjectDownloadToLocalDiskDemo](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/demo/com.ibm.streamsx.objectstorage.file.download.demo)
* [EventStreams2COSDemo](https://github.com/IBMStreams/streamsx.objectstorage/tree/develop/demo/data.historian.event.streams.cos.exactly.once.semantics.demo)





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

The ObjectStorage toolkit contains three generic operators, the *ObjectStorageScan*, *ObjectStorageSource* and the *ObjectStorageSink*
and three S3-specific operators, the *S3ObjectStorageScan*, *S3ObjectStorageSouce* and the *S3ObjectStorageSink*.
In addition, the toolkit contains set of native functions for the bucket and object management in COS (Cloud Object Storage).

Read more about how to use these operators and functions in the [SPL documentaion](/streamsx.objectstorage/doc/spldoc/html/index.html).

### Operators Description

* [Scan Operators](/docs/user/scanoperatorsoverview/)
* [Source Operators](/docs/user/sourceoperatorsoverview/)
* [Sink Operators](/docs/user/sinkoperatorsoverview/)

### Samples

#### Samples with IAM Authentication schema

* [PartitionedParquetSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/PartitionedParquetSample)
* [SinkScanSourceSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/SinkScanSourceSample)
* [TimeRollingPolicySample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/TimeRollingPolicySample)

#### Samples with Basic Authentication schema

* [FunctionsSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/FunctionsSample)
* [PartitionedParquetSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/PartitionedParquetSample)
* [SinkScanSourceSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/SinkScanSourceSample)
* [TimeRollingPolicySample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/TimeRollingPolicySample)


### Demos

* [AvroDemo](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/demo/com.ibm.streamsx.objectstorage.avro.sample)
* [ObjectDownloadDemo](https://github.com/IBMStreams/streamsx.objectstorage/blob/master/demo/com.ibm.streamsx.objectstorage.file.sample/com.ibm.streamsx.objectstorage.file.sample)
* [MH2COSDemo](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/demo/com.ibm.streamsx.objectstorage.messagehub.sample/com.ibm.streamsx.objectstorage.messagehub.sample)



## SPLDOC

[SPLDoc for the com.ibm.streamsx.objectstorage toolkit](https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/index.html)

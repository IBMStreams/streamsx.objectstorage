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

### SPLDOC

Read more about how to use the operators and functions in the [SPL documentaion](/streamsx.objectstorage/doc/spldoc/html/index.html).

### Samples

#### Samples with IAM Authentication schema

* [FunctionsSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/FunctionsSampleIAM)
* [PartitionedParquetSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/PartitionedParquetSampleIAM)
* [SinkScanSourceSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/SinkScanSourceSampleIAM)
* [TimeRollingPolicySample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/iam/TimeRollingPolicySampleIAM)

#### Samples with Basic Authentication schema

* [FunctionsSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/FunctionsSample)
* [PartitionedParquetSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/PartitionedParquetSample)
* [SinkScanSourceSample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/SinkScanSourceSample)
* [TimeRollingPolicySample](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/samples/basic/TimeRollingPolicySample)


### Demos

* [FormatDemo-Avro-Json-Parquet](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/demo/com.ibm.streamsx.objectstorage.formats.demo)
* [ObjectDownloadToLocalDiskDemo](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/demo/com.ibm.streamsx.objectstorage.file.download.demo)
* [EventStreams2COSDemo](https://github.com/IBMStreams/streamsx.objectstorage/tree/master/demo/data.historian.event.streams.cos.exactly.once.semantics.demo)





---
title: "ObjectStorageSink and S3ObjectStorageSink Operators Overview"
permalink: /docs/user/sinkoperatorsoverview/
excerpt: "Describes the usage of the Scan operators."
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

### Operators Description
The toolkit contains two sink operators. `ObjectStorageSink` operator uses generic
parameters approach rather `S3ObjectStorageSink` operator uses S3-compliant parameters.
For example, `ObjectStorageSink` uses `objectStorageURI` paramerer 
which consists of protocol and bucket name (s3a://<BUCKET_NAME>/),  
rather `S3ObjectStorageSink` operator uses S3-compliant parameters such as protocol 
and bucket as a separate parameters making it more intuitive for the users familiar
with S3 COS concepts.

Both operators write tuples that arrive on its input port to the object in COS that is 
named by the `objectName` parameter. You can control whether the object closes the current
output object and creates a new object for writing based on the size of the data written
to the object in bytes, the number of tuples that are written to the object, the time in seconds
that the object is open for writing, or when operator receives a window/final punctuation marker
(so called rolling policy definition). 

The operators support two storage formats:
* parquet - when output object is generated in parquet format
* raw - when output object is generated in the raw format


### Supported Authentication Schemes
The operator supports IBM Cloud Identity and Access Management (IAM) and HMAC for authentication.

For IAM authentication the following authentication parameters should be used:
* IAMApiKey
* IAMServiceInstanceId 
* IAMTokenEndpoint - iam token endpoint. The default is `htts://iam.ng.bluemix.net/oidc/token`.

The following diagram demonstrates how `IAMApiKey` and `IAMServiceInstanceId` can be extracted 
from the COS service credentials:
![Import](/streamsx.objectstorage/doc/images/COSCredentialsOnCOSOperatorMapping.png)

For HMAC authentication the following authentication parameters should be used:
* objectStorageUser
* objectStoragePassword

### Supported Storage Formats
The operators support two storage formats that might be configured with the `storageFormat` parameter.
The `storageFormat` parameter supports two values: `parquet` and `raw`.
Following is the description of operator behavior for each of the storage format options and description
of other parameters that are relevant for each storage format.

#### Parquet Storage Format
Parameters relevant for `parquet` storage format
* `nullPartitionDefaultValue` - Specifies default for partitions with null values.
* `parquetBlockSize` - Specifies the block size which is the size of a row group being buffered in memory. The default is 128M.
* `parquetCompression` - Enum specifying support compressions for parquet storage format. Supported compression types are 'UNCOMPRESSED','SNAPPY','GZIP'
* `parquetDictPageSize` - There is one dictionary page per column per row group when dictionary encoding is used. The dictionary page size works like the page size but for dictionary.
* `parquetEnableDict` - Specifies if parquet dictionary should be enabled.
* `parquetEnableSchemaValidation` - Specifies of schema validation should be enabled.
* `parquetPageSize` - Specifies the page size is for compression. A block is composed of pages. The page is the smallest unit that must be read fully to access a single record. If this value is too small, the compression will deteriorate. The default is 1M.
* `parquetWriterVersion` - Specifies parquet writer version. Supported versions are `1.0` and `2.0`
* `skipPartitionAttributes` - Avoids writing of attributes used as partition columns in data files.
* `partitionValueAttributes` - Specifies the list of attributes to be used for partition column values. Please note, 
that its strongly recommended not to use attributes with continuous values per rolling policy unit of measure 
to avoid operator performance degradation. The following examples demonstrates recommended and non-recommended 
partitioning approaches. 

Recommended: /YEAR=YYYY/MONTH=MM/DAY=DD/HOUR=HH

Non-recommended: /latutide=DD.DDDD/longitude=DD.DDDD/


#### Raw Storage Format
Describe input port schema + dataAttribute + encoding + headerRow + objectNameAttribute

### Operator Parameters

#### ObjectStorageSink

Following are `ObjectStorageSink` operator parameters grouped by category (authentication, connection,
storage format, object rolling policy, etc.).

##### Authentication parameters

###### IAM Authentication

| Parameter Name       | Default | Description                                              |
| -------------------- | ------- | -------------------------------------------------------- |
| IAMApiKey            |  N/A    | Specifies IAM API Key.                                   |
| IAMServiceInstanceId |  N/A    | Specifies IAM token endpoint.                            |
| IAMTokenEndpoint     |  N/A    | Specifies instance id for connection to object storage.  |

Currently, the IAM Authentication mechanism supports IBM COS only.

###### HMAC Authentication

| Parameter Name        | Default | Description                                                                                                           |
| --------------------- | --------| --------------------------------------------------------------------------------------------------------------------- |
| objectStorageUser     | N/A     | Specifies username for HMAC-based authentication to cloud object storage (AKA 'AccessKeyID' for S3-compliant COS).    |
| objectStoragePassword | N/A     | Specifies password for HMAC-based authentication to cloud object storage (AKA 'SecretAccessKey' for S3-compliant COS. |
 
HMAC authentication might be used with IBM and Amazon COS. 
 
##### COS Connection
 
| Parameter Name       | Default | Description                                                                                                       |
| -------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| objectStorageURI     | N/A     | Specifies URI for connection to object storage. The URI should be in 'cos://<bucket>/ or s3a://<bucket>/' format. |
| endpoint             | N/A     | Specifies endpoint for connection to object storage. For example, for S3 the endpoint might be 's3.amazonaws.com'.|

##### Rolling Policy

Rolling policy specifies the window size managed by operator per output object. 
When window is closed the current output object is closed and a new object is opened.

The operator supports three rolling policy types:

* Size-based

| Parameter Name       | Default | Description                                                        |
| -------------------- | ------- | ------------------------------------------------------------------ |
| bytesPerObject       | N/A     | Specifies the approximate size of input data per object, in bytes. | 

* Time-based

| Parameter Name       | Default         | Description                                                                                                                         |
| -------------------- | ----------------| ----------------------------------------------------------------------------------------------------------------------------------- |
| timePerObject        | N/A             | Specifies the approximate time, in seconds, after which the current output object is closed and a new object is opened for writing. |

* Tuple count-based

| Parameter Name       | Default  | Description                              																													 |
| -------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| tuplesPerObject      | N/A      | Specifies the maximum number of tuples per object. When specified number of tuples received, the current output object is closed and a new object is opened. |

##### Object Name

Object name is specified with `objectName` operator parameter.
The `objectName` parameter can optionally contain the following variables, which the operator evaluates at runtime
to generate the object name:

* `%TIME` is the time when the COS object is created. The default time format is yyyyMMdd_HHmmss. 
 
  The variable %TIME can be added anywhere in the path after the bucket name. The variable is typically used to 
  make dynamic object names when you expect the application to create multiple objects. 
  Here are some examples of valid file paths with %TIME:
    - event%TIME.parquet
    - %TIME_event.parquet
    - /my_new_folder/my_new_file_%TIME.csv
    - /geo/uk/geo_%TIME.parquet
    - /geo/uk/my_new_folder/%TIME_event.parquet

* `%OBJECTNUM` is an object number, starting at 0, when a new object is created for writing. 
  Objects with the same name will be overwritten. Typically, %OBJECTNUM is added after the file name.
  Here are some examples of valid file paths with %OBJECTNUM:
    - event_%OBJECTNUM.parquet
    - /geo/uk/geo_%OBJECTNUM.parquet
    - %OBJECTNUM_event.csv
    - %OBJECTNUM_%TIME.csv

  Note: If partitioning is used, %OBJECTNUM is managed globally for all partitions in the COS object, 
  rather than independently for each partition.

* `%PARTITIONS` place partitions anywhere in the object name.  By default, partitions are placed immediately before the last part of the object name.
  Here's an example of default position of partitions in an object name: 
  Suppose that the file path is /GeoData/test_%TIME.parquet. Partitions are defined as YEAR, MONTH, DAY, and HOUR. 
  The object in COS would be /GeoData/YEAR=2014/MONTH=7/DAY=29/HOUR=36/test_20171022_124948.parquet 

  With %PARTITIONS, you can change the placement of partitions in the object name from the default. 
  Let's see how the partition placement changes by using %PARTITIONS:
	Suppose that the file path now is /GeoData/Asia/%PARTITIONS/test_%TIME.parquet. 
	The object name in COS would be 
		/GeoData/Asia/YEAR=2014/MONTH=7/DAY=29/HOUR=36/test_20171022_124948.parquet 

  ** Empty partition values ** 
  If a value in a partition is not valid, the invalid values are replaced by the string __HIVE_DEFAULT_PARTITION__ in the COS object name. 
  For example, /GeoData/Asia/YEAR=2014/MONTH=7/DAY=29/HOUR=__HIVE_DEFAULT_PARTITION__/test_20171022_124948.parquet 

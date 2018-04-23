---
title: "ObjectStorageSource and S3ObjectStorageSource Operators Overview"
permalink: /docs/user/sourceoperatorsoverview/
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
The toolkit contains two source operators. `ObjectStorageSource` operator uses generic
parameters approach rather `S3ObjectStorageSource` operator uses S3-compliant `authentication` and `connection` parameters.
For example, `ObjectStorageSource` uses `objectStorageURI` paramerer 
which consists of protocol and bucket name (s3a://\<BUCKET_NAME\>/),  
rather `S3ObjectStorageSource` operator uses S3-compliant parameter names such as `protocol` 
and `bucket` as a separate parameters making it more intuitive for the users familiar
with S3 COS terms. Note, that other operator parameter groups (except of `authentication` and `connection`) 
are exactly the same for both operators.

Both operators read objects from a Cloud Object Storage (COS).

### Supported Authentication Schemes

The `ObjectStorageSource` operator supports both IBM Cloud Identity and Access Management (IAM) and HMAC for authentication.
The `S3ObjectStorageSource` operator supports HMAC authentication only.

For `ObjectStorageSource` IAM authentication the following authentication parameters should be used:
* IAMApiKey
* IAMServiceInstanceId 
* IAMTokenEndpoint - iam token endpoint. The default is `htts://iam.ng.bluemix.net/oidc/token`.

The following diagram demonstrates how `IAMApiKey` and `IAMServiceInstanceId` can be extracted 
from the COS service credentials:
![Import](/streamsx.objectstorage/doc/images/COSCredentialsOnCOSOperatorMapping.png)

For `ObjectStorageSource` operator HMAC authentication the following authentication parameters should be used:
* objectStorageUser
* objectStoragePassword

For `S3ObjectStorageSource` operator HMAC authentication the following authentication parameters should be used:
* accessKeyID
* secretAccessKey


### Parameters

Following are the `ObjectStorageSource` and the `S3ObjectStorageSource` operator parameters grouped by category (authentication, connection,
source object). Note, that except of `authentication` and `connection` parameter groups both operators
have the same configuration parameters.

##### Authentication parameters

###### IAM Authentication

| Parameter Name       | Default | Description                                              |
| -------------------- | ------- | -------------------------------------------------------- |
| IAMApiKey            |  N/A    | Specifies IAM API Key.                                   |
| IAMServiceInstanceId |  N/A    | Specifies IAM token endpoint.                            |
| IAMTokenEndpoint     |  N/A    | Specifies instance id for connection to object storage.  |

Notes:
 * IAM Authentication mechanism supports IBM COS only
 * IAM Authentication mechanism is supported by `ObjectStorageSource` operator only

###### HMAC Authentication

For `ObjectStorageSource` operator the following authentication parameters should be used:

| Parameter Name        | Default | Description                                                                                                           |
| --------------------- | --------| --------------------------------------------------------------------------------------------------------------------- |
| objectStorageUser     | N/A     | Specifies username for HMAC-based authentication to cloud object storage (AKA 'AccessKeyID' for S3-compliant COS).    |
| objectStoragePassword | N/A     | Specifies password for HMAC-based authentication to cloud object storage (AKA 'SecretAccessKey' for S3-compliant COS. |
 

For `S3ObjectStorageSource` operator the following authentication parameters should be used:

| Parameter Name        | Default | Description                                               |
| --------------------- | --------| --------------------------------------------------------- |
| accessKeyID           | N/A     | Specifies access key id for HMAC-based authentication     |
| secretAccessKey       | N/A     | Specifies secret access key for HMAC-based authentication |


HMAC authentication might be used with IBM and Amazon COS. 
 
##### Connection Parameters 
 
For `ObjectStorageSource` operator the following connection parameters should be used:

| Parameter Name       | Default | Description                                                                                                       |
| -------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| objectStorageURI     | N/A     | Specifies URI for connection to object storage. The URI should be in 'cos://<bucket>/ or s3a://<bucket>/' format. |
| endpoint             | N/A     | Specifies endpoint for connection to object storage. For example, for S3 the endpoint might be 's3.amazonaws.com'.|

For `S3ObjectStorageSource` operator the following connection parameters should be used:

| Parameter Name       | Default | Description                                                                                                       |
| -------------------- | ------- | ------------------------------------------------------------------------------------------ |
| protocol             | N/A     | Specifies protocol to be used for connection to COS. Possible values are  `cos` and `s3a`. |
| bucket               | N/A     | Specifies the bucket name where target objects will be written.                            |
| endpoint             | N/A     | Specifies endpoint for connection to object storage. For example,                          |

##### Source Object Parameters

| Parameter Name | Default | Description                                                                                                                                        |
| ---------------| ------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| objectName     | N/A     | Specifies the name of the object that the operator opens and reads. The parameter must be specified when the optional input port is not configured.|
| initDelay      | 0       | Optional parameter specifies the time to wait in seconds before the operator reads the first object.                                               |
| encoding       | UTF-8   | Specifies the encoding to use when reading objects.                                                                                                |
| blockSize      | 4096    | Specifies the maximum number of bytes to be read at one time when reading an object in binary mode (i.e. into blob). When parameter value is 0 - the full object is loaded at once.|

### Input Ports
The `ObjectStorageSource` and `S3ObjectStorageSource` operators have one optional input port.
If an input port is specified, the operator expects an input tuple with a single attribute of type `rstring` 
containing the name of the object that the operator reads.

### Output Ports
The `ObjectStorageSource` and `S3ObjectStorageSource` operators have one output port.
The tuples on the output port contain the data that is read from the objects. The operators support two reading modes:
 - to read an object line-by-line, the expected output port schema is `tuple<rstring line>` or `tuple<ustring line>`.
 - to read an object as binary, the expected output port schema is `tuple<blob data>`. Use `blockSize` parameter to control
   how much data to retrieve on each read. If `blockSize` parameter value equals to 0 the operator returns the whole object
   as a single tuple. The operator generates a window punctuation marker at the conclusion of each object.


### Metrics

Following table summarizes the list of operators metric:

|Metric		                |Description							                                          |
|---------------------------|---------------------------------------------------------------------------------|
|nObjectsOpened	            | The number of objects that are opened by the operator for reading data.         |

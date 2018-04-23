---
title: "ObjectStorageScan and S3ObjectStorageScan Operators Overview"
permalink: /docs/user/scanoperatorsoverview/
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
The `ObjectStorageScan` and `S3ObjectStorageScan` operators scan a COS bucket or its subfolder for new or modified objects.

### Object Storage - Supported Authentication Schemes
The `ObjectStorageScan` operator supports both IBM Cloud Identity and Access Management (IAM) and HMAC for authentication.
The `S3ObjectStorageScan` operator supports HMAC authentication only.

For `ObjectStorageScan` IAM authentication the following authentication parameters should be used:
* IAMApiKey
* IAMServiceInstanceId 
* IAMTokenEndpoint - iam token endpoint. The default is `htts://iam.ng.bluemix.net/oidc/token`.

The following diagram demonstrates how `IAMApiKey` and `IAMServiceInstanceId` can be extracted 
from the COS service credentials:
![Import](/streamsx.objectstorage/doc/images/COSCredentialsOnCOSOperatorMapping.png)

For `ObjectStorageScan` operator HMAC authentication the following authentication parameters should be used:
* objectStorageUser
* objectStoragePassword

For `S3ObjectStorageScan` operator HMAC authentication the following authentication parameters should be used:
* accessKeyID
* secretAccessKey


### Parameters

Following are the `ObjectStorageScan` and the `S3ObjectStorageScan` operator parameters grouped by category (authentication, connection,
scanning). Note, that except of `authentication` and `connection` parameter groups both operators
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
 * IAM Authentication mechanism is supported by `ObjectStorageSink` operator only

###### HMAC Authentication

For `ObjectStorageSink` operator the following authentication parameters should be used:

| Parameter Name        | Default | Description                                                                                                           |
| --------------------- | --------| --------------------------------------------------------------------------------------------------------------------- |
| objectStorageUser     | N/A     | Specifies username for HMAC-based authentication to cloud object storage (AKA 'AccessKeyID' for S3-compliant COS).    |
| objectStoragePassword | N/A     | Specifies password for HMAC-based authentication to cloud object storage (AKA 'SecretAccessKey' for S3-compliant COS. |
 

For `S3ObjectStorageSink` operator the following authentication parameters should be used:

| Parameter Name        | Default | Description                                               |
| --------------------- | --------| --------------------------------------------------------- |
| accessKeyID           | N/A     | Specifies access key id for HMAC-based authentication     |
| secretAccessKey       | N/A     | Specifies secret access key for HMAC-based authentication |


HMAC authentication might be used with IBM and Amazon COS. 
 
##### Connection Parameters 
 
For `ObjectStorageSink` operator the following connection parameters should be used:

| Parameter Name       | Default | Description                                                                                                       |
| -------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| objectStorageURI     | N/A     | Specifies URI for connection to object storage. The URI should be in 'cos://<bucket>/ or s3a://<bucket>/' format. |
| endpoint             | N/A     | Specifies endpoint for connection to object storage. For example, for S3 the endpoint might be 's3.amazonaws.com'.|

For `S3ObjectStorageSink` operator the following connection parameters should be used:

| Parameter Name       | Default | Description                                                                                                       |
| -------------------- | ------- | ------------------------------------------------------------------------------------------ |
| protocol             | N/A     | Specifies protocol to be used for connection to COS. Possible values are  `cos` and `s3a`. |
| bucket               | N/A     | Specifies the bucket name where target objects will be written.                            |
| endpoint             | N/A     | Specifies endpoint for connection to object storage. For example,                          |
|                      |         | for Amazon S3 the endpoint might be 's3.amazonaws.com'.                                    |

##### Scanning Parameters

| Parameter Name | Default | Description                                                                                                                       |
| ---------------| ------- | --------------------------------------------------------------------------------------------------------------------------------- |
| pattern        | *       | Optional parameter limits the object name that are listed to the names that match the specified regular expression.               |
| directory      | /       | Optional parameter specifies the name of the directory to be scanned. Directory should always be considered in context of bucket. |
| initDelay      | 0       | Optional parameter specifies the time to wait in seconds before the operator scans the bucket  for the first time.                |
| sleepTime      | 5.0     | Optional parameter specifies the minimum time between directory scans.                                                            |  
| strictMode     | false   | Optional parameter determines whether the operator should report an error if the bucket (or directory in it) to be scanned does not exists. | 
                             


### Input Ports

Both `ObjectStorageScan` and `S3ObjectStorageScan` operators have an optional control input port.
The port can be used to change the directory under the same bucket that the operator scans at the runtime 
without stopping the job. The expected schema for the input port is `<rstring dir>` (the attribute name doesn't 
matter), a single attribute of type `rstring`. If a directory scan is in progress when a tuple received, the 
scan completes and a new scan starts immediately after and uses the new directory that was specified in the input
tuple. If the operator is sleeping, the operator starts scanning the new directory immediately after it receives
an input tuple.

### Output Ports

Both `ObjectStorageScan` and `S3ObjectStorageScan` operators have one mandatory output port. The port output 
schema is `<rstring objectName>` that are encoded in UTF-8 and represents the object names that are found in
the bucket folder, one tuple per object. 


### Metrics

| Name       | Type    | Description                                                                                                       |
| -------------------- | ----------------------------------------------------------- |
| nScans     | Counter | The number of times the operator scanned the bucker folder. |

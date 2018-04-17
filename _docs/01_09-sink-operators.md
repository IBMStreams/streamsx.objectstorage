---
title: "ObjectStorageScan and S3ObjectStorageScan Operators Overview"
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
which consists of protocol and bucket name (s3a://<BUCKET_NAME>\/>),  
rather `S3ObjectStorageSink` operator uses S3-compliant parameters such as protocol 
and bucket as a separate parameters making it more intuitive for the users familiar
with S3 COS concepts.


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


### Supported SPL Types


### Parameters

| Parameter Name | Default | Description |
| --- | --- | --- |
| closeOnPunct | | Close object on punctuation. |


### Consistent Region Support


#### Drain

#### Checkpoint

#### Reset

#### ResetToInitialState


### Input Ports


### Output Ports


### Error Handling

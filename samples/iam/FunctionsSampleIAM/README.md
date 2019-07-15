# Functions and Operator Sample

## Description

The sample application shows the usage of the operators and functions to access Object Storage.
A bucket is created, sample data read and written as several small objects to Object Storage, then read from Object Storage.
Finally the objects and the bucket is deleted.

Additional purpose of the sample is 
to demonstrate how to configure `ObjectStorageSource` and `ObjectStorageSink` operator 
with IAM-authentication type.

Either set `cos.creds` containing COS credentials JSON in `cos` application configuration or set the JSON in the `credentials` parameter.

## Utilized Toolkits
 - com.ibm.streamsx.objectstorage

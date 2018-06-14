# Dynamic Object Name Sink Sample

## Description

This sample application demonstrates how to use the ObjectStorageSink operator with dynamic object name and close on Window marker.

Additional purpose of the sample is 
to demonstrate how to configure `ObjectStorageSink` operator 
with IAM-authentication type.

Either set `cos.creds` containing COS credentials JSON in `cos` application configuration or set `IAMApiKey` and `IAMServiceInstanceId` parameters.

## Utilized Toolkits
 - com.ibm.streamsx.objectstorage

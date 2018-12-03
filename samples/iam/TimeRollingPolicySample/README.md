# Time Rolling Policy Sample with IAM Authentication

## Description
The sample demonstrates how to configure `ObjectStorageSink` operator with time-based rolling policy,
i.e. to close output object approximately every `$timePerObject` seconds. 
Note, that in addition, the example demonstrates how to use `%TIME` 
variable in the output object name. 

Additional purpose of the sample is 
to demonstrate how to configure `ObjectStorageSink` operator 
with IAM-authentication type.

Either set `cos.creds` containing COS credentials JSON in `cos` application configuration or set the JSON in the `credentials` parameter.

## Utilized Toolkits
 - com.ibm.streamsx.objectstorage

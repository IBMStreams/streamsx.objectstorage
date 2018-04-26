# Time Rolling Policy Sample with IAM Authentication

## Description
The sample demonstrates how to configure `ObjectStorageSink` operator with
IAM authentication credentials with time-based rolling policy,
i.e. to close output object approximately every `$timePerObject` seconds. 
Note, that in addition, the example demonstrates how to use `%TIME` 
variable in the output object name. 
    
## Utilized Toolkits
 - com.ibm.streamsx.objectstorage
# SinkScanSource Sample with IAM Authentication

## Description
The sample demonstrates how to configure 
`ObjectStorageSink/ObjectStorageScan/ObjectStorageSource` operators with the HMAC credentials.
In addition, the sample demonstrates the following topics:
   - writing output object by binary blocks of 1K size by `ObjectStorageSink` operator
   - utilization of `%OBJECTNUM` variable by `ObjectStorageSink` operator's `objectName` parameter
   - utilization of complex pattern by `ObjectStorageScan` operator. The sample pattern is `SAMPLE_[0-9]*\\.ascii\\.text$`.
    
## Utilized Toolkits
 - com.ibm.streamsx.objectstorage
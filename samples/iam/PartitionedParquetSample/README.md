# Partitioned Parquet Sample

## Description
The sample demonstrates partitioned output creation
in parquet format. 

Additional purpose of the sample is 
to demonstrate how to configure `ObjectStorageSink` operator 
with IAM-authentication type.

Either set `cos.creds` containing COS credentials JSON in `cos` application configuration or set `IAMApiKey` and `IAMServiceInstanceId` parameters.


Note, that the sample uses records from `etc/partitionSampleData.txt` 
file as an input. The input consists of timestamped network events.
The timestamp string is parsed to `<int32 YEAR, int32 MONTH,  int32 DAY, int32 HOUR>`
structure which used later on for partitioning by `ObjectStorageSink` operator.

## Utilized Toolkits
 - com.ibm.streamsx.objectstorage

# MessageHub to Object Storage Demo

## Description
The demo demonstrates the very common use case when input
data is read from Message Hub and is written to the COS.

The demo consists of two examples each one corresponsing to the different
output storage format:

1. `M2HObjectStorageCSVSample`

The demo uses `com.ibm.streamsx.messagehub::MessageHubConsumer`
operator for reading data from the message hub and generation
of the input tuples. The demo assumes that the data consumed from the message
hub has a textual format.  
The downstream `Format` operator formats the string consumed from the message
hub to the `blob` binary format which then is written as-is (with no additional formatting)
in the `raw` format to COS using `ObjectStorageSink` operator.

2. `M2HObjectStoragePartitionParquetSample`

The demo uses `com.ibm.streamsx.messagehub::MessageHubConsumer`
operator for reading data from the message hub and generation
of the input tuples. The demo assumes that the data consumed from the message
hub has a `json` format. The downstream `Custom` and `Functor` operators parse 
the input json to tuple of `DataHistorianData_t` type and extend the input tuple 
with additional attributes (`continent` and `city`) required for partitioning definition.

Note, that for both examples `MHDataProducer` composite is used for input generation.
`MHDataProducer` generates tuples of `DataHistorianData_t` type using 
`com.ibm.streamsx.messagehub::MessageHubProducer` operator and sends them
to the message hub in the `json` format.

## Utilized Toolkits
 - com.ibm.streamsx.json
 - com.ibm.streamsx.messagehub
 - com.ibm.streamsx.objectstorage
# Read from Event Streams and write to COS

## Description

This test case reads data from one partitioned topic with multiple MessageHub consumers within a consumer group.
The number of partitions need not be known as the topic partitions are assigned to the MessageHub consumers by Kafka.

Data is written to Cloud Object Storage without any processing in between in parquet format. The number of parallel writers can be configured.

The consistent region period is 60 seconds and cannot be configured as a submission time value.


![Import](/demo/data.historian.event.streams.cos.exactly.once.semantics.demo/doc/images/dh_mh2cos.png)


## Requirements

com.ibm.streamsx.objectstorage toolkit version 1.6 or above
com.ibm.streamsx.messagehub toolkit version 1.5 or above

Both toolkits must be found in the STREAMS_SPLPATH for the Makefile to work.

For running the application, two app configs are required for the credentials for the cloud services.

### Service credentials for Event Streams (Message Hub)

app option name = `messagehub`
property name = `messagehub.creds`

Put the entire JSON string into the property value.

### Service credentials for Cloud Object Storage

app option name = `cos`
property name = `cos.creds`

Put the entire JSON string into the property value.


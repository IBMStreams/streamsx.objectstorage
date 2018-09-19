# Read from MessageHub and write to COS

## Description

This test case reads data from one partitioned topic with multiple MessageHub consumers within a consumer group. The number of partitions need not be known as the topic partitions are assigned to the MessageHib consumers by Kafka.

Data is wirtten to Cloud Object Storage without any processing in between in raw format. The number of parallel writers can be configured.

The consitent region period is 10 seconds and cannot be configured as a submission time value.

## Requirements

com.ibm.streamsx.objectstorage toolkit version 1.6 or above
com.ibm.streamsx.messagehub toolkit version 1.5 or above

Both toolkits must be found in the STREAMS_SPLPATH for the Makefile to work.

For running the application, two app configs are required for the credentials for the cloud services.

### Service credentials for Message Hub

app option name = `messagehub`
property name = `messagehub.creds`

Put the entire JSON string into the property value.

### Service credentials for Cloud Object Storage


app option name = `cos`
property name = `cos.creds`

Put the entire JSON string into the property value.


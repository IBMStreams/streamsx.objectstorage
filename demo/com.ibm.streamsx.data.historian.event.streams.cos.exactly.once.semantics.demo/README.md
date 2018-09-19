# Event Streams to Object Storage Demo

## Description

The demo demonstrates the very common use case when input
data is read from Event Streams and is written to the IBM Cloud Object Storage (COS).

## Write generate data to Event Streams

### Launch the Data Generator app to the Streaming Analytics service

streamsx-runner --service-name $STREAMING_ANALYTICS_SERVICE_NAME --main-composite com.ibm.streamsx.datahistorian.generate.json::Main --toolkits dh_generate_json --submission-parameters mh.topic=dh6 mh.topic.numPartitions=6 numMessages.per.partition=4000000

## "Event Streams to COS" app to the Streaming Analytics service

### Prepare environment variables

`export COS_URI="s3a://<YOUR_BUCKET_NAME>"`

Replace <YOUR_BUCKET_NAME> with your target COS bucket.

`export MH_TOOLKIT=<MESSAGEHUB_TOOLKIT_LOCATION>`

Replace <MESSAGEHUB_TOOLKIT_LOCATION> with the toolkit location of the com.ibm.streamsx.messagehub toolkit (version 1.5).

`export MH_TOOLKIT=../../../streamsx.messagehub/com.ibm.streamsx.messagehub`

For example:
`export COS_TOOLKIT=<COS_TOOLKIT_LOCATION>`

Replace <COS_TOOLKIT_LOCATION> with the toolkit location of the com.ibm.streamsx.objectstorage toolkit (version 1.6).

For example:
`export COS_TOOLKIT=../../com.ibm.streamsx.objectstorage`

### Launch "Event Streams to COS" app to the Streaming Analytics service

streamsx-runner --service-name $STREAMING_ANALYTICS_SERVICE_NAME --main-composite com.ibm.streamsx.datahistorian.json.parquet::Main --toolkits dh_json_parquet $MH_TOOLKIT $COS_TOOLKIT --trace info --submission-parameters mh.consumer.group.size=6 mh.topic=dh6 cos.number.writers=4 cos.uri=$COS_URI


## Utilized Toolkits
 - com.ibm.streamsx.json
 - com.ibm.streamsx.messagehub
 - com.ibm.streamsx.objectstorage

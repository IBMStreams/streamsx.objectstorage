# Launch the applications for the Data Historian Event Streams to Object Storage Demo

Either run the application [dh_generate_json](dh_generate_json/README.md) on a dedicated Streaming Analytics service instance or generate the test data on the same Streaming Analytics service, wait for completion and cancel the job before launching the  [dh_json_parquet](dh_json_parquet/README.md) application.

## 1) Write generated data to Event Streams

#### Streaming Analytics service (lite plan) and Event Streams (standard plan)

Before launching the application, you need to update the SPL file [dh_generate_json/com.ibm.streamsx.datahistorian.generate.json/Main.spl](dh_generate_json/com.ibm.streamsx.datahistorian.generate.json/Main.spl) and set the period to `5.0`.

    @consistent (trigger = periodic, period = 5.0, ...

*This update is required for Event Streams using standard plan only!*

When running a **"Lite"** plan Streaming Analytics service, then set the amount of data to 1000000 messages and the number of partitions to one!

    streamsx-runner --service-name $STREAMING_ANALYTICS_SERVICE_NAME --main-composite com.ibm.streamsx.datahistorian.generate.json::Main --toolkits dh_generate_json --submission-parameters mh.topic=dh_lite mh.topic.numPartitions=1 numMessages.per.partition=1000000


## 2) "Event Streams to COS" app to the Streaming Analytics service (lite plan)

### Prepare environment variables

`export COS_URI="s3a://<YOUR_BUCKET_NAME>"`

Replace <YOUR_BUCKET_NAME> with your target COS bucket.

Set the toolkit location of the com.ibm.streamsx.messagehub toolkit (version 1.5):

`export MH_TOOLKIT=../../../streamsx.messagehub/com.ibm.streamsx.messagehub`

Set the toolkit location of the com.ibm.streamsx.objectstorage toolkit (version 1.6).

For example:
`export COS_TOOLKIT=../../com.ibm.streamsx.objectstorage`

### Launch "Event Streams to COS" app to the Streaming Analytics service (lite plan)

When running a **"Lite"** plan Streaming Analytics service, then set the number of consumers and writers to one:

    streamsx-runner --service-name $STREAMING_ANALYTICS_SERVICE_NAME --main-composite com.ibm.streamsx.datahistorian.json.parquet::Main --toolkits dh_json_parquet $MH_TOOLKIT $COS_TOOLKIT --trace info --submission-parameters mh.consumer.group.size=1 mh.topic=dh_lite cos.number.writers=1 cos.uri=$COS_URI


The command above launches the application read from Event Streams with the topic name *`dh_lite`* using *`1`* consumer and writing to COS using *`1`* writer.


## 3) Validate the data integrity 

### Simulate error cases 

Restart any Processing Element (PE) of the dh_json_parquet application while running in the Streaming Analytics service.

The [Job Control Plane](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/jobcontrolplane.html) will reset the region and the operators recover from their checkpoints. Consumer operators proceed processing with last saved checkpoint.

### Verify with IBM SQL Query service that all messages are written to COS exactly once 

Open the UI of the SQL query service and run the following queries to check that the expected 1000000 messages are processed with exactly once semantics.

Replace the bucket name "datahistorian-001" with your bucket name in the queries below.

    SELECT COUNT(id) FROM cos://us-geo/datahistorian-001/DataHistorian/*.parquet STORED AS PARQUET

Validate that the messages are distinct:

    SELECT COUNT(DISTINCT id, channel) FROM cos://us-geo/datahistorian-001/DataHistorian/*.parquet STORED AS PARQUET 

The result should look like this: 

    COUNT(DISTINCT ID, CHANNEL)
    1000000




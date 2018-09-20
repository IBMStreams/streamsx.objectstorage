# Setting up the IBM Cloud services for the Data Historian Demo

You'll need:
  * An instance of [IBM COS](https://console.bluemix.net/docs/services/cloud-object-storage/getting-started.html)
  * An instance of [IBM Event Streams](https://console.bluemix.net/docs/services/EventStreams/index.html#getting_started)
  * An instance of [IBM Streaming Analytics](https://console.bluemix.net/docs/services/StreamingAnalytics/index.html#gettingstarted)

## Connect Streaming Analytics service with Event Streams and COS

Generate and get the credentials:
  * [generating Event Streams 'service credential'](https://console.bluemix.net/docs/services/MessageHub/messagehub127.html#connecting)
  * [generating COS 'service credential'](https://console.bluemix.net/docs/services/cloud-object-storage/iam/service-credentials.html)

These credentials shall be stored in IBM Streaming Analytics service instance application configuration properties.

### Service credentials for Event Streams

app option name = `messagehub`
property name = `messagehub.creds`

Put the entire JSON string into the property value.

### Service credentials for Cloud Object Storage

app option name = `cos`
property name = `cos.creds`

Put the entire JSON string into the property value.

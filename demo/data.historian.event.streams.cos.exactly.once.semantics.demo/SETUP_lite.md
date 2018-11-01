# Setting up the IBM Cloud services for the Data Historian Demo using Streaming Analytics lite service plan

You'll need:
  * An instance of [IBM COS](https://console.bluemix.net/docs/services/cloud-object-storage/getting-started.html)
  * An instance of [IBM Event Streams](https://console.bluemix.net/docs/services/EventStreams/index.html#getting_started)
  * An instance of [IBM Streaming Analytics](https://console.bluemix.net/docs/services/StreamingAnalytics/index.html#gettingstarted)
  * An instance of [IBM SQL Query](https://console.bluemix.net/docs/services/sql-query/getting-started.html#getting-started-tutorial)

**"Lite" service plans for the demo use case**:
* Standard plan for Event Streams
* Lite Container for Streaming Analytics
* Lite plan for IBM Cloud Object Storage
* Select the **same region** for the services, for example, `us-south` 

For IBM SQL Query service the lite plan is sufficient.

## Connect Streaming Analytics service with Event Streams and COS

Generate and get the credentials:
  * [generating Event Streams 'service credential'](https://console.bluemix.net/docs/services/MessageHub/messagehub127.html#connecting)
  * [generating COS 'service credential'](https://console.bluemix.net/docs/services/cloud-object-storage/iam/service-credentials.html)

These credentials shall be stored in IBM Streaming Analytics service instance [application configuration](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/creating-secure-app-configs-dev.html)
 properties.

### Service credentials for Event Streams

Setup the [Message Hub toolkit operators]((https://ibmstreams.github.io/streamsx.messagehub/doc/spldoc/html/tk$com.ibm.streamsx.messagehub/ns$com.ibm.streamsx.messagehub$1.html):

app option name = `messagehub`

property name = `messagehub.creds`

Put the entire JSON string into the property value.

### Service credentials for Cloud Object Storage

Setup the [Objectstorage toolkit operators]((https://ibmstreams.github.io/streamsx.objectstorage/doc/spldoc/html/tk$com.ibm.streamsx.objectstorage/tk$com.ibm.streamsx.objectstorage$1.html):

app option name = `cos`

property name = `cos.creds`

Put the entire JSON string into the property value.


## Create a topic in Event Streams service

When running the **"lite"** scenario, it is recommended to create a topic wiht one partition only.

topic name = `dh_lite`

partitions = `1`


## Create bucket in Cloud Object Storage service

Create a bucket with a unique bucket name, for example `dh-demo001`, `cross-region` for location: `us-geo` with `Standard` Storage class.

## Prepare toolkits

* Download [streamsx.messagehub toolkit](https://github.com/IBMStreams/streamsx.messagehub). For this demo at least version 1.5 is required. You need to build the toolkit, if no pre-built release is available.
* Download [streamsx.objectstorage toolkit](https://github.com/IBMStreams/streamsx.objectstorage). For this demo at least version 1.6 is required. You need to build the toolkit, if no pre-built release is available.


The toolkits containing the demo applications needs to be indexed before launching them with [streamsx-runner](http://ibmstreams.github.io/streamsx.topology/doc/pythondoc/scripts/runner.html) 

    make tkidx

After this toolkit.xml files in `dh_generate_json` and `dh_json_parquet`directories have been generated.

## Prepare streamsx-runner

### Python

Ensure that you have Python 3.5 or later installed. For example, you can get Python 3.5 from the [Anaconda archive page](https://repo.continuum.io/archive/index.html).

Ensure that the bin directory is added to the PATH environment variable. If necessary, add the bin directory by entering the following command on the command line:

    export PATH="~/anaconda3/bin:$PATH"

### Install streamsx-runner

    pip install streamsx

### Prepare environment variables

Ensure that you have set the following environment variables for testing with Streaming Analytics service in IBM Cloud:

* `STREAMING_ANALYTICS_SERVICE_NAME` - name of your Streaming Analytics service
* `VCAP_SERVICES` - [VCAP](https://console.bluemix.net/docs/services/StreamingAnalytics/r_vcap_services.html#r_vcap_services) information in JSON format or a JSON file





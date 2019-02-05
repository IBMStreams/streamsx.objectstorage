# Example Python Application for Data Historian Demo

The IBM Streams Python Application API enables you to create streaming analytics applications in Python.
The python script `dh.py` contains a **Python Topology Sample Application** using the `streamsx` package.

## Required python packages

* streamsx
* streamsx.objectstorage
* streamsx.eventstreams

Initial install from "pypi.org":

    pip install streamsx
    pip install streamsx.objectstorage
    pip install streamsx.eventstreams

Upgrade to latest versiom, if you have it already installed.

    pip install --upgrade streamsx
    pip install --upgrade streamsx.objectstorage
    pip install --upgrade streamsx.eventstreams

## Prepare environment variables

Ensure that you have set the following environment variables for launching the app to Streaming Analytics service in IBM Cloud:

| Environment variable | content |
| --- | --- |
| STREAMING_ANALYTICS_SERVICE_NAME | name of your Streaming Analytics service |
| VCAP_SERVICES | [VCAP](https://console.bluemix.net/docs/services/StreamingAnalytics/service_plans.html#v2_vcap_services) information in JSON format or a JSON file |
| EVENT_STREAMS_TOPIC | Topic name in the Event Streams service |
| EVENTSTREAMS_SERVICE_CREDENTIALS | Optional: Event Streams service credentials in JSON. If not specified the default application configuration is used |
| MH_TOOLKIT | The directory where the com.ibm.streamsx.messagehub toolkit is located |
| COS_TOOLKIT | The directory where the com.ibm.streamsx.objectstorage toolkit is located |
| COS_BUCKET | The name of the bucket |
| COS_ENDPOINT | Endpoint to COS. For example (private region us-south): s3.us-south.objectstorage.service.networklayer.com |
| COS_SERVICE_CREDENTIALS | Optional: COS service credentials in JSON. If not specified the default application configuration is used. |



## Launch the applications

    python3 dh.py

# Data Historian - Event Streams to Object Storage Demo

## Description

The demo demonstrates the very common use case when input
data is read from Event Streams and is written to the IBM Cloud Object Storage (COS).
These objects created on COS can be queried with IBM SQL Query service.

![Import](/demo/data.historian.event.streams.cos.exactly.once.semantics.demo/doc/images/dh_overview.png)

The demo contains two IBM Streams applications:

* Data generator application [dh_generate_json](dh_generate_json/README.md)
* Event Streams to COS application [dh_json_parquet](dh_json_parquet/README.md)

## Requirements

## Launch the applications

Instructions how to launch the applications to the Streaming Analytics service: [Launch](LAUNCH.md)

## Utilized Toolkits
 - com.ibm.streamsx.json
 - com.ibm.streamsx.messagehub
 - com.ibm.streamsx.objectstorage

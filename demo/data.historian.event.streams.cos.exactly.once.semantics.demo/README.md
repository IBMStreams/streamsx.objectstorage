# Data Historian - Event Streams to Object Storage Demo

## Description

The demo demonstrates the very common use case when input
data is read from Event Streams and is written to the IBM Cloud Object Storage (COS).
These objects created on COS can be queried, for example, with IBM SQL Query service.

The demo applications integrate IBM Streams features, like [consistent region](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/consistentregions.html), [user-defined parallelism](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.dev.doc/doc/udpoverview.html) and [optional data types](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.3.0/com.ibm.streams.ref.doc/doc/optional.html), and provide the following features:
* Scalability
* Data integrity - Guaranteed processing with exactly once semantics
* Resiliency
* Simplicity - Takes advantage of [Kafka consumer groups](https://kafka.apache.org/intro#intro_consumers)

![Import](/demo/data.historian.event.streams.cos.exactly.once.semantics.demo/doc/images/dh_overview.png)

The demo contains two IBM Streams applications:

* Data generator application [dh_generate_json](dh_generate_json/README.md)
* Event Streams to COS application [dh_json_parquet](dh_json_parquet/README.md)

## Requirements

Setup the IBM Cloud services: [Setup](SETUP.md)

## Launch the applications

Instructions how to launch the applications to the Streaming Analytics service: [Launch](LAUNCH.md)

## Utilized Toolkits
 - com.ibm.streamsx.json
 - com.ibm.streamsx.messagehub
 - com.ibm.streamsx.objectstorage

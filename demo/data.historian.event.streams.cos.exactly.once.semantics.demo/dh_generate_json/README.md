# Data generator to put JSON data into Event Streams

## Description

Generates tuples with several attributes and random data, converts them to JSON and writes the JSON data into Event Streams in several partitions of a single topic.

![Import](/demo/data.historian.event.streams.cos.exactly.once.semantics.demo/doc/images/dh_generator.png)


## Requirements

com.ibm.streamsx.messagehub toolkit version 1.4.2 or above

For running the application, one app config is required for the credentials for the cloud services.

### Service credentials for Event Streams (Message Hub)

Setup the [Message Hub toolkit operators](https://ibmstreams.github.io/streamsx.messagehub/doc/spldoc/html/tk$com.ibm.streamsx.messagehub/ns$com.ibm.streamsx.messagehub$1.html):

app option name = `messagehub`
property name = `messagehub.creds`

Put the entire JSON string into the property value.

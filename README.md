# streamsx.objectstorage

The **com.ibm.streamsx.objectstorage** toolkit provides primitive operators and native functions for reading and writing data from and to Object Storage.
This toolkit supports S3 compatible Object Storages and allows developers to write IBM Streams application that interacts with [IBM
Cloud Object Storage](https://console.bluemix.net/docs/services/cloud-object-storage/about-cos.html). 

## Documentation

Find the full documentation [here](https://ibmstreams.github.io/streamsx.objectstorage/).

## Changes
[CHANGELOG.MD](com.ibm.streamsx.objectstorage/CHANGELOG.md).

## Quick start with IBM Cloud Object Storage

You'll need:
  * An instance of [IBM COS](https://console.bluemix.net/docs/services/cloud-object-storage/getting-started.html).
  * An API key from [IBM Cloud Identity and Access Management](https://console.bluemix.net/docs/iam/users_roles.html) with at least `Writer` permissions.
  * The ID of the instance of COS that you are working with.
  * Token acquisition endpoint
  * Service endpoint

These values can be found in the IBM Cloud UI by [generating a 'service credential'](https://console.bluemix.net/docs/services/cloud-object-storage/iam/service-credentials.html) and these credentials shall be stored in an IBM Streams instance application configuration property.

## Streaming Analytics service on IBM Cloud

This toolkit is compatible with the Streaming Analytics service on IBM Cloud.

### Python package 

There is a python package available, that exposes SPL operators in the `com.ibm.streamsx.objectstorage` toolkit as Python methods.
* [streamsx.objectstorage python package](https://pypi.org/project/streamsx.objectstorage/)
* [Python package documentation](http://streamsxobjectstorage.readthedocs.io)

### Demo application with integration of other IBM Cloud services 

*Event Streams* --> *COS* [Demo](demo/data.historian.event.streams.cos.exactly.once.semantics.demo/README.md)



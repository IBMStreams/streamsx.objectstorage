---
title: "Toolkit technical background overview"
permalink: /docs/knowledge/overview/
excerpt: "Basic knowledge of the toolkits technical domain."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "knowledgedocs"
---
{% include toc %}
{% include editme %}

Do you have a Streams application that you’re interested in running in the cloud?

If your application reads from the local disk or creates new files, then using one of the Object Storage services on Bluemix and the new toolkit could be the solution to make your files available in the cloud.

With Object Storage, your unstructured data is stored in a scalable, multi-tenant cloud environment.

You can now use the new Object Storage toolkit to access objects in Object Storage from Streams.  It provides the following features:

* Create/Delete bucket (S3)/container (Swift)
* Put/Get object
* Delete object
* List objects

## Streaming Analytics service on IBM Bluemix

This toolkit is compatible with the Streaming Analytics service on Bluemix.

[Introduction to the Bluemix Streaming Analytics service](https://developer.ibm.com/streamsdev/docs/streaming-analytics-now-available-bluemix-2/)

### Object Storage service

* [Object Storage service catalog](https://console.ng.bluemix.net/catalog/services/object-storage?cm_sp=dw-bluemix-_-streamsdev-_-devcenter)
* [Object Storage service](https://console.ng.bluemix.net/docs/services/ObjectStorage/os_works_public.html)

### Cloud Object Storage service

* [IBM Cloud Object Storage service catalog](https://console.bluemix.net/catalog/infrastructure/object-storage-group)

## API Reference

The *com.ibm.streamsx.objectstorage* toolkit supports Object Storage services with S3 API or Swift API.

* [Object Storage (Swift) API](https://developer.openstack.org/api-ref/object-storage/index.html)
* [IBM COS (S3) API](https://ibm-public-cos.github.io/crs-docs/java#api-reference)

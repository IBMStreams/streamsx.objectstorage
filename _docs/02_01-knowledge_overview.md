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

If your application reads from the local disk or creates new files, then using one of the Object Storage services on IBM Cloud and the new toolkit could be the solution to make your files available in the cloud.

With Object Storage, your unstructured data is stored in a scalable, multi-tenant cloud environment.

You can now use the new Object Storage toolkit to access objects in Object Storage from Streams.  It provides the following features:

* Create/Delete bucket
* Put/Get object
* Delete object
* List objects

## Streaming Analytics service on IBM Cloud

This toolkit is compatible with the Streaming Analytics service on IBM Cloud.

[Introduction to the IBM Cloud Streaming Analytics service](https://developer.ibm.com/streamsdev/docs/streaming-analytics-now-available-bluemix-2/)

### Cloud Object Storage service

* [IBM Cloud Object Storage service catalog](https://console.bluemix.net/catalog/infrastructure/object-storage-group)

## API Reference

The *com.ibm.streamsx.objectstorage* toolkit supports Object Storage services with S3 API.

* [IBM COS (S3) API](https://console.bluemix.net/docs/infrastructure/cloud-object-storage-infrastructure/about-api.html#about-the-cos-api)

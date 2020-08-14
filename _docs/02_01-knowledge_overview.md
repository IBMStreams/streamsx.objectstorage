---
title: "Toolkit technical background overview"
permalink: /docs/knowledge/overview/
excerpt: "Basic knowledge of the toolkits technical domain."
last_modified_at: 2020-08-14T11:17:48-04:00
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

* [IBM Cloud Object Storage service](https://cloud.ibm.com/docs/cloud-object-storage)


## Cloud Object Storage Basic Terms

**Object Storage Background**

Objects are similar to files, but there are important differences. Like files, objects have both data and metadata, although object metadata is much richer.
Unlike files, object data is written once and never modified; you cannot append or update data to an object although you can overwrite it completely.
Unlike filesystems, object storage has a flat namespace, although directory hierarchies can be simulated by using some delimiter such as forward slashes ‘/’ in object names.

There is no rename operation for objects, so renaming objects can only be done by rewriting the entire object with a new name and deleting the old one.


**Bucket**

A bucket is a logical abstraction that is used to provide a container for data. Buckets in COS are created in IBM Cloud. 
For example, you might create a bucket called `blackfriday` to be the container for all streaming data from Black Friday 
store sales and another bucket called `postxmas` for store sales on 26-Dec. You create a third bucket called `clickstream`
to contain streaming data about all online sales activity.
 
**Partition**

A partition is data that is grouped by a common attribute in the incoming schema.
Use partitions when you need to reduce the amount of data that queries must process. 
Streaming gives you access to massive amounts of data. Querying the entire data set might 
not be possible or even necessary. To improve query performance, break the data into chunks,
or partitions, and just query the chunk that you need. For example, we might want to get information 
about online shopping users who put an item into their cart. Our first partition is 
`click_event_type` so that we can query on the clickstream `add_to_cart` event. 
Next, we add the partition `customer_id` because we want to analyze each customer's online shopping behavior.




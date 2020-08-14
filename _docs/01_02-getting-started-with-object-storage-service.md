---
title: "Getting Started with Object Storage service"
permalink: /docs/user/objectstorageservice
excerpt: "How to use this toolkit."
last_modified_at: 2020-08-14T12:02:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}


## Quick start with IBM Cloud Object Storage (COS) service

You'll need:

* An instance of the IBM COS service
* IBM COS service credentials created
* A bucket created that you will either read to or write from.


The streamsx.objectstorage toolkit supports the Cloud Object Storage service on IBM Cloud:

![Import](/streamsx.objectstorage/doc/images/COS_lite.png)


Select the [Cloud Object Storage](https://console.bluemix.net/catalog/infrastructure/object-storage-group) service, choose "Lite" as pricing plan and create it.

When the service is created, you need to create the service credentials. Select the "Writer" access role.

![Import](/streamsx.objectstorage/doc/images/cos_service_credentials.png)

## Create a bucket

![Import](/streamsx.objectstorage/doc/images/createBucket.png)

## Steps to complete

### Create an application configuration

The credentials for the COS service are stored in the Streams instance in an application configuration.  Create an application configuration by following these steps.

### Get your endpoint (optional)

If your bucket is not created in us-geo location, then select the [endpoint](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-endpoints#endpoints) for the corresponding region. Specify the endpoint using the endpoint parameter.

When running your application in the Streaming Analytics service (IBM Cloud), you can use private endpoints to access your bucket. Otherwise the public endpoints shall be used.

### Develop your application

Configure the ObjectStorageSink/Scan/Source operators with the right parameters.  For example, the application below uses the ObjectStorageSink to write objects in Parquet format, creating a new file every timePerObject seconds.

### Launch the application

Since the COS credentials are retrieved from the application configuration, you need to launch the application in distributed mode. If you are not using the default endpoint, you can specify the endpoint as the submission parameter `os-endpoint` in the sample application.


```
composite Main {
   param
      expression<rstring> $objectStorageURI: getSubmissionTimeValue("os-uri", "cos://streams-sample-001/");
      expression<rstring> $endpoint: getSubmissionTimeValue("os-endpoint", "s3-api.us-geo.objectstorage.softlayer.net");
      expression<float64> $timePerObject: 10.0; // Objects are created in parquet format after $timePerObject in seconds

   type
      S3ObjectStorageSinkOut_t = tuple<rstring objectName, uint64 size>;

   graph

      stream<rstring username, uint64 id> SampleData = Beacon() {
         param
            period: 0.1;
         output
            SampleData : username = "Test"+(rstring) IterationCount(), id = IterationCount() ;
      }

      // Operator reads IAM credentials from application configuration.
      // Ensure that cos application configuration with property cos.creds has been created.
      
      stream<S3ObjectStorageSinkOut_t> ObjStSink = com.ibm.streamsx.objectstorage::ObjectStorageSink(SampleData) {
         param
         objectStorageURI: $objectStorageURI;
         endpoint : $endpoint;
         objectName: "sample_%TIME.snappy.parquet";
         timePerObject : $timePerObject;
         storageFormat: "parquet";
         parquetCompression: "SNAPPY";
      }

      () as SampleSink = Custom(ObjStSink as I) {
         logic
         onTuple I: {
            printStringLn("Object with name '" + I.objectName + "' of size '" + (rstring)I.size + "' has been created.");
         }
      }
}
```


## View Created Files

After running the application for a few minutes, you can view or download the created files from the IBM Cloud dashboard.

Select your Object Storage instance in your IBM Cloud dashboard and select your bucket `streams-sample-001` in the buckets menu in order to list the objects in the bucket. 

![Import](/streamsx.objectstorage/doc/images/bucketObjects.png)

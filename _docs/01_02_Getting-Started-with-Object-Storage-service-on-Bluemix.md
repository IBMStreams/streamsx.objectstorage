---
title: "Getting Started with Object Storage service"
permalink: /docs/user/overview/objectstorageservice
excerpt: "How to use this toolkit."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

The streamsx.objectstorage toolkit supports the following Object Storage services on Bluemix:

![Import](/streamsx.objectstorage/doc/images/COS.png)
![Import](/streamsx.objectstorage/doc/images/Object Storage.png)

This guide describes the steps for the [Object Storage](https://console.ng.bluemix.net/docs/services/ObjectStorage/os_works_public.html) service.
Select the service, choose "Free" as pricing plan and create it.

When the service is created, you need to create the service credentials.

![Import](/streamsx.objectstorage/doc/images/Credentials.png)

The sample application of the toolkit (**com.ibm.streamsx.objectstorage.swift.sample**) has the following submission time parameters that needs to be set with the credentials marked in red.

* ObjectStorage-ProjectId
* ObjectStorage-UserId
* ObjectStorage-Password

If you have selected London as region, then you need to apply the `ObjectStorage-AccessPoint` parameter with the value `lon.objectstorage.open.softlayer.com`. Dallas is the default region.



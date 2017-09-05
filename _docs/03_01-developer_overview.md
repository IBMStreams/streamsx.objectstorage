---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}


# Prerequisite

This toolkit uses Apache Ant 1.8 (or later) to build.

Internally Apache Maven 3.2 (or later) and Make are used.

Download and setup directions for Apache Maven can be found here: http://maven.apache.org/download.cgi#Installation

Set environment variable M2_HOME to the path of maven home directory.

    export M2_HOME=/opt/apache-maven-3.5.0
    export PATH=$PATH:$M2_HOME/bin

## Download and build the streamsx.inet toolkit

Run the following command in the `streamsx.objectstorage` directory:

    ant downloadInetToolkit

After this step the `com.ibm.streamsx.inet` directory resides in the same directory as the `streamsx.objectstorage`.

The **streamsx.inet** toolkit is required since the **streamsx.objectstorage** toolkit uses the functions
* com.ibm.streamsx.inet.http::httpGet
* com.ibm.streamsx.inet.http::httpDelete
* com.ibm.streamsx.inet.http::httpPost
* com.ibm.streamsx.inet.http::httpPut


# Build the toolkit

Run the following command in the `streamsx.objectstorage` directory:

    ant all

# Build the samples

Run the following command in the `streamsx.objectstorage` directory to build all projects in the `samples` directory:

    ant build-all-samples

